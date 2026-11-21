# 0003. At-least-once com idempotência e checkpoint, em vez de exactly-once transacional

- Status: Aceito
- Data: 2026-11-21

## Contexto

O agregador lê leituras, mantém janelas de 1 minuto em memória e publica agregados, alertas e mensagens de DLQ. Um crash ou rebalance não pode perder leituras nem gerar agregados duplicados ou parciais no Postgres. O Kafka oferece exactly-once (EOS) via transações (read-process-write com `sendOffsetsToTransaction`), mas os destinos finais (Postgres, data lake) estão fora da transação.

## Decisão

Usar **at-least-once com consumidor idempotente**:

1. Commit manual de offset só depois de publicar (com ack) tudo o que o lote gerou e de gravar o checkpoint, nessa ordem.
2. O offset commitado nunca passa do primeiro registro de uma janela aberta (offset seguro), então um crash não perde leituras ainda não agregadas.
3. O checkpoint (offset seguro + watermark) vai para um tópico compactado. Ao retomar uma partição, o watermark restaurado descarta como atrasadas as leituras de janelas já emitidas, e as janelas abertas são reconstruídas inteiras. Resultado: o reprocessamento reemite exatamente os mesmos agregados.
4. Deduplicação por `event_id` cobre retransmissões dos gateways.
5. Saídas têm identidade determinística (estufa + grandeza + início da janela; `alert_id` derivado da janela e da regra) e os sinks fazem upsert.

## Consequências

- Duplicatas no tópico de agregados podem existir (crash entre publish e commit), mas são idênticas e o upsert as absorve: efeito exatamente uma vez no Postgres.
- Funciona com qualquer destino que aceite upsert ou deduplicação, inclusive fora do Kafka.
- Sem o custo de transações (coordenador, marcadores, consumidores `read_committed` esperando o LSO).
- O checkpoint é uma peça a mais para operar, e o tópico é relido a cada rebalance.
- Leituras atrasadas além da tolerância são descartadas (contadas em métrica), não reprocessadas.

## Alternativas consideradas

- **EOS transacional do Kafka**: garante atomicidade entre produzir agregados e commitar offsets, mas só dentro do Kafka; o sink JDBC continuaria precisando de upsert. Exige `transactional.id` estável por partição, aumenta latência e complica o tratamento de fencing. Fica como evolução se surgir um consumidor de agregados que não tolere duplicatas.
- **Commit automático**: simples, mas pode commitar antes do processamento e perder dados num crash.
- **Commitar o último offset processado (sem offset seguro)**: perde as leituras de janelas abertas num crash.
- **Kafka Streams / ksqlDB**: resolveriam janelas e estado com changelog, mas exigem JVM; aqui o objetivo é mostrar o mecanismo em Go.
