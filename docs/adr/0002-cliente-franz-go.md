# 0002. franz-go como cliente Kafka

- Status: Aceito
- Data: 2026-11-21

## Contexto

Produtor e consumidor são em Go. Precisamos de produtor idempotente com `acks=all`, particionador compatível com o cliente Java (para a chave de estufa cair na mesma partição em qualquer linguagem), consumidor em grupo com commit manual e controle sobre o momento dos rebalances, além de um cliente de Schema Registry.

## Decisão

Usar **franz-go** (`kgo`, `kadm`, `sr`), cliente Kafka em Go puro.

## Consequências

- Sem cgo: build estático, imagem `distroless` e cross-compile triviais.
- Idempotência ligada por padrão; `StickyKeyPartitioner` usa murmur2 como o cliente Java.
- `BlockRebalanceOnPoll` garante que um rebalance nunca acontece no meio do processamento de um lote, o que simplifica muito a consistência entre estado em memória e offsets.
- `CommitOffsetsSync` dá commit síncrono e explícito; o franz-go não grava metadata no commit, por isso o watermark vai para um tópico de checkpoint (ADR 0003).
- Comunidade menor que a do librdkafka; parte da documentação está só nos comentários do código.

## Alternativas consideradas

- **confluent-kafka-go**: o mais próximo do cliente oficial e com serdes de Schema Registry prontos, mas depende de librdkafka via cgo, complica a imagem e o cross-compile.
- **segmentio/kafka-go**: API simples, mas idempotência e controle de rebalance mais limitados.
- **Sarama**: maduro, mas API mais verbosa e histórico de bugs em consumer groups.
