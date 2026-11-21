# 0008. Agregados e alertas no Postgres via sink JDBC com upsert

- Status: Aceito
- Data: 2026-11-21

## Contexto

O painel da cooperativa consulta séries de 1 minuto por estufa e o histórico de alertas com SQL. O agregador publica em tópicos; alguém precisa materializar isso no banco, com a mesma garantia de idempotência do resto do pipeline.

## Decisão

- Kafka Connect com o **JDBC sink** da Confluent, configuração versionada em `connect/*.json` e aplicada com `PUT /connectors/<nome>/config` (idempotente) por `scripts/deploy-connectors.sh`.
- `insert.mode=upsert` com chave primária de negócio: `(greenhouse_id, sensor_type, window_start)` para agregados e `alert_id` para alertas.
- Tabelas criadas por migração (`deploy/postgres/init.sql`), com `auto.create=false`: o banco não muda de forma implícita quando o schema evolui.
- Senha do banco via `EnvVarConfigProvider` (`${env:JDBC_SINK_PASSWORD}`), nunca no JSON.

## Consequências

- Reemissões de agregados (at-least-once) viram updates idempotentes.
- Nenhum código próprio de escrita no banco; retries e paralelismo ficam com o Connect.
- Mudança de schema que adiciona coluna exige migração antes; é deliberado.
- O JDBC sink não trata erro de banco com DLQ (só erros de conversão), então falha de banco para a task, que precisa de alerta.

## Alternativas consideradas

- **Escrever no Postgres dentro do agregador**: acopla o caminho quente ao banco e mistura responsabilidades.
- **Debezium/outbox**: resolve o caminho inverso (banco para Kafka), não este.
- **auto.create=true**: prático em demo, perigoso em produção.
