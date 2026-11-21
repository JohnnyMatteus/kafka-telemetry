# Teste de carga

## Objetivo

Medir quanto o pipeline aguenta numa máquina de desenvolvimento e confirmar que a garantia de entrega não degrada sob carga: o agregador precisa acompanhar o produtor (lag volta a zero depois do pico) e o produtor idempotente com `acks=all` precisa manter latência de ack baixa.

## Como rodar

```bash
make up
make loadtest LOAD_RATE=2000 LOAD_DURATION=60s LOAD_GREENHOUSES=200
```

`scripts/load-test.sh`:

1. pausa o simulador do compose para não misturar carga;
2. compila e roda o próprio simulador com a taxa, a duração e o número de estufas pedidos (1% de retransmissões e 0,1% de leituras inválidas);
3. lê `/metrics` do agregador a cada segundo, guardando o lag máximo;
4. depois do fim da carga, mede quanto tempo o lag leva para zerar;
5. grava o relatório em `loadtest-results/<timestamp>.txt` (fora do Git).

O simulador imprime ao final as leituras confirmadas, os erros, o throughput efetivo e a latência de ack p50/p95/p99/máximo (do enqueue até a confirmação de todas as réplicas em sincronia).

## Por que o próprio simulador e não k6

O gargalo que interessa é o caminho Kafka (produtor idempotente, partições, consumidor). O k6 fala HTTP, e a extensão de Kafka (xk6-kafka) exigiria build customizado do k6 e reimplementar a serialização Avro com o formato de fio do registry. O simulador já produz mensagens idênticas às de produção, com a mesma chave de partição, e mede a latência de ack no ponto certo.

## Ambiente

Notebook de desenvolvimento (macOS, Docker Desktop com 6,8 GB para todos os containers), Kafka 3.9 (Confluent Platform 7.9.1) em nó único KRaft, 6 partições em `greenhouse.readings`, RF=1.

## Resultados observados

**A medição formal ainda não foi executada.** Durante a construção, o Docker da máquina estava compartilhado com os ambientes de outros projetos do portfólio e, para economizar recursos, a rodada de carga foi adiada para o CI/uma máquina dedicada. O script e as métricas estão prontos; esta seção será preenchida com a saída de `make loadtest`.

O que foi observado no ambiente completo, com a carga padrão do compose (20 estufas, 80 leituras/s, 2% de retransmissões, 0,5% de inválidas, 10% de estufas superaquecidas):

| Observação | Resultado |
| --- | --- |
| `telemetryctl init` | 5 tópicos criados; 4 versões de schema registradas (ids 1 a 4) com BACKWARD |
| Leituras inválidas | Enviadas para `greenhouse.readings.dlq` com `reason=invalid_reading` e partição/offset de origem |
| Alertas | Emitidos para as estufas superaquecidas (`gh-001`, `gh-002`) a cada janela, média ≈ 37,6 °C contra limite de 32 °C |
| Janelas | Fechadas cerca de 10 s depois do fim de cada minuto, como esperado pela tolerância |

Problemas encontrados e corrigidos nessa rodada: o healthcheck do Kafka herdava o heap do broker e acumulava JVMs sob CPU escassa; um `NOT_COORDINATOR` transitório derrubava o agregador (agora erros retentáveis são só registrados).

## Leitura dos resultados

Métricas a acompanhar na rodada formal: throughput efetivo do produtor contra o pedido (se ficar abaixo, o gargalo é o produtor ou o broker), latência de ack p99, lag máximo do agregador e tempo de drenagem depois do fim da carga. O agregador processa cada partição em sequência num único laço; se o lag não drenar, o próximo passo é escalar instâncias até o número de partições, já que o estado é por partição e o checkpoint permite a troca de dono sem perda.

## Próximos passos

- Rodar no CI noturno com um limiar de regressão de throughput.
- Medir com 3 brokers, RF=3 e `min.insync.replicas=2`, onde `acks=all` custa uma ida e volta de replicação.
- Escalar o agregador para várias instâncias (até o número de partições) e medir o tempo de rebalance com checkpoint.
