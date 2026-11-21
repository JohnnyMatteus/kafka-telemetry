# 0006. Kafka em modo KRaft no ambiente local e Redpanda nos testes de integração

- Status: Aceito
- Data: 2026-11-21

## Contexto

O ambiente local precisa de Kafka, Schema Registry e Kafka Connect próximos de produção. Os testes de integração precisam subir rápido, no laptop e no CI, sem orquestrar vários containers.

## Decisão

- `docker-compose.yml` com **Confluent Platform 7.9.1** (Kafka 3.9) em **modo KRaft** (nó único broker+controller, sem ZooKeeper), Schema Registry e Connect da mesma versão.
- Testes de integração com **Redpanda** via testcontainers: um container fala o protocolo Kafka e expõe uma API compatível com o Schema Registry. Os testes usam build tag `integration` e são pulados sem Docker.

## Consequências

- KRaft é o modo suportado daqui para frente (ZooKeeper foi removido no Kafka 4.0).
- Testes de integração sobem em segundos com um único container.
- Redpanda não é Kafka: diferenças de comportamento (por exemplo, configs de tópico aceitas) podem esconder problemas; o ambiente compose cobre o Kafka real, e o teste de carga roda nele.

## Alternativas consideradas

- **Kafka + ZooKeeper**: legado.
- **apache/kafka nos testes + Schema Registry separado**: mais fiel, porém dois containers e mais tempo de subida; candidato para um job noturno.
- **Mocks do cliente Kafka**: não testam rebalance, commit nem formato de fio.
