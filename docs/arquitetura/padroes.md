# Padrões e princípios no código

Só entra aqui o que o código tem de fato. Cada item aponta o trecho, o problema que resolve e o custo.

## Camadas

| Camada | Pacote | Depende de |
| --- | --- | --- |
| Domínio | `internal/telemetry` (leitura, janelas, deduplicação, limites) | nada de infraestrutura, nem relógio de parede |
| Caso de uso | `internal/aggregation` (`Service` e as portas em `ports.go`) | domínio e as próprias portas |
| Adaptadores | `internal/kafka`, `internal/state`, `internal/contract`, `internal/observability` | caso de uso (implementam as portas) e bibliotecas |
| Composição | `cmd/aggregator`, `cmd/simulator`, `cmd/telemetryctl` | tudo; é onde as peças são ligadas |

## Padrões

### Portas e adaptadores (hexagonal)

- **Onde**: as portas `Publisher`, `CheckpointStore`, `Committer`, `Decoder`, `Encoder`, `StateStore` e `Observer` em [internal/aggregation/ports.go](../../internal/aggregation/ports.go#L47); os adaptadores em [internal/kafka/publisher.go](../../internal/kafka/publisher.go#L20), [internal/kafka/checkpoint.go](../../internal/kafka/checkpoint.go#L39), [internal/kafka/consumer.go](../../internal/kafka/consumer.go#L179) e [internal/state/redis.go](../../internal/state/redis.go#L50).
- **Problema**: a garantia de entrega (publish → checkpoint → commit) precisa ser testada com falhas em cada passo. Com portas, `service_test.go` simula falha de publish, de checkpoint e de commit em memória, sem broker.
- **Trade-off**: mais tipos e uma camada de tradução (`aggregation.Message` ↔ `kgo.Record`). Os testes de integração continuam necessários para provar que o adaptador respeita o contrato da porta.

### Adapter

- **Onde**: [kafka.Publisher](../../internal/kafka/publisher.go#L20) traduz `[]aggregation.Message` em `kgo.Record` com headers; [state.RedisStore](../../internal/state/redis.go#L50) traduz `SensorState` num campo JSON do hash da estufa.
- **Problema**: o caso de uso fala a própria língua; franz-go e go-redis ficam isolados num arquivo cada.
- **Trade-off**: uma cópia por mensagem na tradução, irrelevante perto do custo de rede.

### Repository

- **Onde**: [kafka.CheckpointStore](../../internal/kafka/checkpoint.go#L39) guarda e recupera checkpoints num tópico compactado (`Save`/`Load`); [state.RedisStore](../../internal/state/redis.go#L77) faz o mesmo para o último estado das estufas.
- **Problema**: o serviço pede "o checkpoint de cada partição" sem saber que ele mora num tópico compactado lido do início.
- **Trade-off**: `Load` relê o tópico inteiro a cada rebalance; aceitável porque é uma mensagem por partição.

### Decorator

- **Onde**: [state.Instrumented](../../internal/state/instrumented.go#L29) envolve qualquer `aggregation.StateStore` e conta as gravações por resultado (`telemetry_aggregator_state_updates_total`). Montado em [cmd/aggregator/main.go](../../cmd/aggregator/main.go#L121).
- **Problema**: métrica sem acoplar o adaptador do Redis ao Prometheus nem aumentar a interface `Observer`.
- **Trade-off**: mais um tipo; a métrica conta estados, não comandos Redis.

### Null Object

- **Onde**: [aggregation.NopObserver](../../internal/aggregation/ports.go#L97), usado quando nenhum `Observer` é injetado ([service.go](../../internal/aggregation/service.go#L76)).
- **Problema**: o serviço chama o observador sem `if obs != nil` espalhado.
- **Trade-off**: nenhum relevante. A projeção de estado é opcional de outro jeito (`State == nil` desliga), porque ali não há chamadas espalhadas.

### Idempotência com ledger e lock (reprocessador da DLQ)

- **Onde**: [dlq.Reprocessor](../../internal/dlq/dlq.go) e [dlq.RedisLedger](../../internal/dlq/redis.go); decisão no [ADR 0012](../adr/0012-reprocessador-da-dlq.md).
- **Problema**: republicar uma mensagem da DLQ duas vezes, ou duas execuções ao mesmo tempo, duplicaria leituras. O ledger registra cada offset republicado, o lock (`SET NX` com TTL e liberação por comparação) serializa as execuções.
- **Trade-off**: entre o publish e o ledger há uma janela em que um crash causa republicação; o deduplicador do agregador a absorve dentro do watermark. O lock não é renovado, então a execução inteira roda num contexto de 60 s, menor que o TTL.

### Backfill idempotente com fotografia (reprocessador da DLQ)

- **Onde**: [dlq/backfill.go](../../internal/dlq/backfill.go) e o [ADR 0015](../adr/0015-backfill-de-janelas-fechadas.md). O estado de cada janela (fotografia do agregado emitido + acumulado + conjunto de `event_id`) é atualizado por um script atômico no Redis; o agregado publicado é sempre derivado da fotografia.
- **Problema**: recalcular uma janela fechada sem somar duas vezes a mesma leitura, mesmo com falha entre somar e publicar.
- **Trade-off**: o estado vive no Redis; perdê-lo faz o próximo backfill partir do Postgres, que já tem o resultado anterior.

### Estratégia por canal e idempotência de entrega (notificador)

- **Onde**: a porta [notify.Channel](../../internal/notify/notify.go) com os adaptadores [EmailChannel](../../internal/notify/email.go) e [WebhookChannel](../../internal/notify/webhook.go); [notify.Service](../../internal/notify/notify.go) escolhe os canais de cada produtor; [RedisSent](../../internal/notify/redis.go) lembra o que já saiu. Decisão no [ADR 0013](../adr/0013-notificador-de-alertas.md).
- **Problema**: acrescentar um canal sem mexer na regra de quem avisar nem na de não repetir.
- **Trade-off**: marcar depois de enviar prefere um aviso repetido a um aviso perdido; falha transitória vai para uma fila agendada no Redis com idade máxima ([retry.go](../../internal/notify/retry.go)), em vez de perder o aviso ao commitar o offset.

### Retry com backoff exponencial

- **Onde**: [Service.decodeWithRetry](../../internal/aggregation/service.go#L246).
- **Problema**: o Schema Registry fora do ar não pode mandar mensagens válidas para a DLQ. O decoder classifica o erro (definitivo vai para a DLQ; transitório tenta de novo, até 5 s entre tentativas).
- **Trade-off**: a partição fica bloqueada enquanto o registry estiver fora; o lag cresce e o alerta de lag avisa.

### Projeção de leitura (CQRS leve)

- **Onde**: [Service.updateState](../../internal/aggregation/service.go#L285) grava o último agregado de cada estufa no Redis depois do publish; [state.Handler](../../internal/state/handler.go#L15) serve `GET /greenhouses/{id}/state`. O script Lua em [redis.go](../../internal/state/redis.go#L19) só aceita janela igual ou mais nova.
- **Problema**: "como está a estufa agora?" sem varrer o Postgres nem o tópico.
- **Trade-off**: a projeção pode ficar uma janela atrás se o Redis falhar (a falha é contada e registrada, não trava o pipeline). Não é fonte da verdade.

### Mapper entre domínio e contrato

- **Onde**: [contract.NewSensorReadingRecord, NewWindowAggregateRecord e NewThresholdAlertRecord](../../internal/contract/records.go#L23).
- **Problema**: o schema Avro evolui (v1 → v2) sem mudar os tipos do domínio.
- **Trade-off**: duplicação de campos entre `telemetry.Aggregate` e `WindowAggregateRecord`.

### Não usados de propósito

- **Outbox**: o destino é o próprio Kafka; a ordem publish → checkpoint → commit com upsert no destino resolve sem tabela intermediária ([ADR 0003](../adr/0003-at-least-once-com-idempotencia.md)).
- **Chain of Responsibility nas validações**: `Reading.Validate` acumula todos os problemas numa passada, o que dá uma mensagem de DLQ completa; uma cadeia pararia no primeiro.
- **Cache do Redis na frente do registry**: o `Decoder` já guarda os schemas em memória por id ([codec.go](../../internal/contract/codec.go#L55)); schema registrado é imutável, então não há invalidação a fazer.

## SOLID

| Princípio | Onde | Problema que resolve | Custo |
| --- | --- | --- | --- |
| **S**: responsabilidade única | `telemetry` só decide (janela, dedup, limite); `aggregation.Service` só orquestra; `kafka.GroupConsumer` só cuida do laço de poll e do grupo ([consumer.go](../../internal/kafka/consumer.go#L101)) | Mudar o cliente Kafka não toca a regra de janela | Mais pacotes para navegar |
| **O**: aberto/fechado | Novos destinos da projeção entram como outro `StateStore`; métricas entram por decorador ([instrumented.go](../../internal/state/instrumented.go#L13)) sem editar o serviço | Acrescentar comportamento sem reabrir o caso de uso testado | Indireção |
| **L**: substituição de Liskov | Os fakes de `service_test.go` e os adaptadores reais cumprem o mesmo contrato documentado na porta (ex.: `Publisher` só retorna depois do ack, [ports.go](../../internal/aggregation/ports.go#L45)); o teste de integração com Redpanda confere o real | O teste em memória vale para produção | O contrato precisa estar escrito, senão os fakes mentem |
| **I**: segregação de interfaces | Portas de um ou dois métodos (`Committer`, `Publisher`, `StateStore`); `state.Reader` separado do `StateStore`, porque o handler só lê ([state.go](../../internal/state/state.go#L43)) | Cada consumidor depende só do que usa | Mais nomes |
| **D**: inversão de dependência | O caso de uso define as portas e o `main` injeta os adaptadores via `aggregation.Deps` ([cmd/aggregator/main.go](../../cmd/aggregator/main.go#L127)) | O núcleo não importa franz-go nem go-redis | A ligação manual no `main` cresce com cada dependência |
