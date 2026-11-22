# 0012. Reprocessador da DLQ

- Status: Aceito
- Data: 2026-11-22

## Contexto

A DLQ guarda o que o agregador recusou: mensagem malformada, schema desconhecido, leitura inválida. Algumas dessas causas passam (um schema registrado depois, um bug de produtor corrigido), e hoje a única saída é reenviar à mão. Um reprocessador mal desenhado vira um vetor: republicar para o tópico errado, repetir o lote, ou apagar a autoria de quem mexeu.

## Decisão

- Caso de uso em `internal/dlq`, com portas para a fonte (DLQ no Kafka), o publisher (o mesmo do agregador) e o ledger (Redis). Exposto por `POST /dlq/replay` (token de operador, ADR 0011) e por `telemetryctl dlq replay`.
- **Destino fixo**: `DLQ_REPLAY_TOPIC` (padrão `greenhouse.readings`). O pedido não tem campo de destino e o decodificador recusa campos desconhecidos. O destino nunca pode ser a própria DLQ. Só mensagens com `dlq.source.topic` igual ao destino são elegíveis.
- **Dry-run por padrão**: sem `dry_run:false` (API) ou `-execute` (comando) nada é publicado nem marcado.
- **Limite de lote**: padrão 10; teto `DLQ_REPLAY_MAX_BATCH` (100, no máximo 1000). Acima do teto o pedido é recusado, não cortado em silêncio.
- **Idempotência**: um ledger no Redis registra o offset de cada mensagem republicada; uma execução seguinte as pula. Um lock com TTL impede duas execuções simultâneas, e um cursor evita reler o prefixo já resolvido.
- **Autoria**: o operador vem do token (API) ou de `-operator` (comando, gravado como `cli:nome`) e é obrigatório para gravar. Vai para o recibo do ledger, para headers da mensagem (`dlq.replayed.by`, `dlq.replay.run`, `dlq.replay.source.offset`) e para o log.
- **Leitura atrasada não é republicada.** A causa corrigida costuma ser descoberta horas depois, quando a janela da leitura já fechou: o agregador a descartaria como `late`, e republicar só produziria um falso sucesso. Antes de escolher uma mensagem, o reprocessador a decodifica e compara a janela com o watermark do último checkpoint da partição de origem (`telemetry.WindowConfig.Size`, via `WINDOW_SIZE`). Janela fechada vira `would_be_late` no resultado (dry-run ou não), sem publicar e sem marcar no ledger; mensagem que ainda não decodifica vira `undecodable`.
- **Prazo e lock**: a execução inteira roda sob um contexto de 60 s, menor que o TTL de 2 min do lock, que não é renovado. Só a gravação do ledger depois de um publish bem-sucedido escapa do prazo (5 s, com teto), para a mensagem nunca sair sem recibo.
- Ordem: publicar com ack, depois gravar o ledger.

## Consequências

- Se o processo cair entre o publish e o ledger, a execução seguinte republica o lote. O agregador deduplica por `event_id` enquanto o evento está dentro do watermark; fora dele a leitura chega atrasada e é descartada e contada. Preferimos isso a perder mensagem (gravar o ledger antes).
- O principal limite: **este reprocessador só recupera leituras de janelas ainda abertas**. Recuperar as mais antigas exige um caminho de backfill (recalcular a janela histórica e fazer upsert do agregado, idempotente por `event_id`), que não existe e fica como decisão pendente do autor. Até lá, as bloqueadas continuam na DLQ à vista do operador.
- A classificação usa o watermark do checkpoint, que pode estar um lote atrás do real: uma leitura classificada como elegível ainda pode ser descartada se o watermark andar antes de ela ser consumida. O oposto não acontece (o watermark só avança).
- Uma mensagem que continua inválida volta para a DLQ como entrada nova (offset novo, header de origem do tópico de leituras), então pode ser republicada de novo se alguém repetir. O filtro `reason` e o dry-run servem para o operador decidir; não há contador de tentativas.
- A API de reprocessamento exige Redis. Sem `REDIS_ADDR`, a rota não existe; o comando falha com mensagem clara.
- O comando confia na declaração de `-operator`: quem o roda já tem acesso direto ao broker e ao Redis. A autoria forte é a da API.
- Sem papéis: qualquer operador com token pode reprocessar (ADR 0011).

## Alternativas

- **Backfill de janelas fechadas**: recalcularia a janela e faria upsert (o sink JDBC já é upsert), mas muda a garantia de publicação do agregador e a semântica do tópico de agregados; fica para uma decisão própria.
- **Destino escolhido no pedido**: flexível, mas um operador (ou um token vazado) poderia injetar mensagens em qualquer tópico.
- **Idempotência só pelo `event_id` do agregador**: o deduplicador é em memória e esquece após o watermark.
- **Ledger no Postgres**: durável, mas o Redis já está no agregador e o lock vem de graça; os recibos são pequenos e reconstruíveis a partir dos headers.
- **Reprocessar automaticamente por motivo**: esconderia erros que precisam de um humano.
