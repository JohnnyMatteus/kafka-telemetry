# API de consulta

JSON em `snake_case`, servida pelo agregador na porta de HTTP (`METRICS_ADDR`, padrão `:9102`; no compose, `AGGREGATOR_METRICS_PORT`). Decisão em [ADR 0010](adr/0010-api-de-consulta-no-agregador.md). Horários em RFC3339 UTC.

Todas as rotas exigem `Authorization: Bearer <token>` de um operador (decisão em [ADR 0011](adr/0011-autenticacao-de-operador-na-api.md)). Sem token ou com token inválido, `401` com `WWW-Authenticate: Bearer`. Sem `OPERATOR_TOKENS` no agregador, nenhuma rota da API existe.

Erros têm o formato `{"error": "mensagem"}`. `400` para parâmetro inválido, `404` para recurso inexistente, `409` para conflito e `503` quando a fonte de dados (Redis, Postgres ou Kafka) está indisponível. Rotas cuja fonte não está configurada não existem (`404`).

| Rota | Fonte |
| --- | --- |
| `GET /greenhouses`, `GET /greenhouses/{id}/state` | Redis |
| `GET /greenhouses/{id}/series`, `GET /alerts`, `POST /alerts/{id}/ack` | Postgres |
| `GET /dlq`, `GET /pipeline/health` | Kafka |
| `GET /thresholds` | configuração |

## GET /greenhouses

Último estado de todas as estufas, ordenadas por id. `sensors` tem as chaves `TEMPERATURE`, `HUMIDITY`, `CO2` e `LUMINOSITY` presentes na estufa; cada valor é o mesmo objeto de `GET /greenhouses/{id}/state`.

```json
{"greenhouses":[{"greenhouse_id":"gh-001","sensors":{"TEMPERATURE":{
  "window_start":"2026-03-10T12:00:00Z","window_end":"2026-03-10T12:01:00Z",
  "sample_count":12,"mean":36.1,"min":35.2,"max":37.0,"unit":"celsius",
  "alert":{"rule":"ABOVE_MAX","observed":36.1,"limit":32}}}}]}
```

`alert` só aparece quando a janela gerou alerta. `GET /greenhouses/{id}/state` devolve `404` para estufa sem estado.

## GET /greenhouses/{id}/series?from=&to=

Agregados por janela com `window_start` em `[from, to)`, por tipo de sensor e em ordem de tempo. `to` padrão é agora; `from` padrão é `to` menos 1 hora. `from` deve ser anterior a `to` e o intervalo não passa de 31 dias. Estufa sem dados no intervalo devolve `series` vazio.

```json
{"greenhouse_id":"gh-001","series":{"TEMPERATURE":{"unit":"celsius","points":[
  {"window_start":"2026-03-10T12:00:00Z","window_end":"2026-03-10T12:01:00Z","sample_count":12,"mean":20.1,"min":19.4,"max":21.0}]}}}
```

## GET /alerts?status=&limit=

Alertas mais recentes primeiro. `status` é `open` (sem reconhecimento), `acknowledged` ou omitido (todos). `limit` padrão 50, máximo 500.

```json
{"alerts":[{"alert_id":"9f2c...","greenhouse_id":"gh-001","sensor_type":"TEMPERATURE",
  "window_start":"2026-03-10T12:00:00Z","window_end":"2026-03-10T12:01:00Z","rule":"ABOVE_MAX",
  "observed_value":36.1,"limit_value":32,"acknowledged_at":null,"note":null}]}
```

`rule` é `ABOVE_MAX` ou `BELOW_MIN`. `acknowledged_at`, `acknowledged_by` e `note` são sempre presentes, `null` quando ausentes. `acknowledged_by` é o operador que reconheceu.

## POST /alerts/{id}/ack

Corpo `{"note":"texto"}` (a nota é opcional, até 500 caracteres). Devolve o alerta atualizado, no formato acima, com `acknowledged_by` igual ao operador do token. O corpo não aceita o nome do operador: ele vem só do token. `404` se o alerta não existe e `409` se já foi reconhecido; a operação é atômica, então dois operadores simultâneos recebem um `200` e um `409`.

## GET /dlq?limit=

Mensagens mais recentes da DLQ de leituras, da mais nova para a mais antiga. `limit` padrão 50, máximo 500. `reason` e `error` vêm dos headers que o agregador grava; `source_*` apontam para a leitura original (`-1` se o header não existe).

```json
{"messages":[{"partition":0,"offset":7,"timestamp":"2026-03-10T12:00:03Z","key":"gh-001",
  "reason":"INVALID_READING","error":"valor fora da faixa","source_topic":"greenhouse.readings",
  "source_partition":2,"source_offset":99}]}
```

## GET /thresholds

Faixa agronômica usada nos alertas, a mesma configuração do agregador.

```json
{"thresholds":{"TEMPERATURE":{"min":12,"max":32,"unit":"celsius"},"HUMIDITY":{"min":50,"max":90,"unit":"percent"},
  "CO2":{"min":350,"max":1500,"unit":"ppm"},"LUMINOSITY":{"min":0,"max":80000,"unit":"lux"}}}
```

Configuração por variável de ambiente: `THRESHOLD_<TIPO>_MIN` e `THRESHOLD_<TIPO>_MAX`, com `<TIPO>` em `TEMPERATURE`, `HUMIDITY`, `CO2`, `LUMINOSITY`. Os padrões estão em `telemetry.DefaultThresholds`. Mudar exige reiniciar o agregador; mínimo >= máximo impede a partida.

## GET /pipeline/health

Formato livre, pensado para painel. Lag vem do broker (offset commitado contra o final da partição, por isso reflete a janela aberta e não só o atraso de leitura); a vazão é a média de registros consumidos nos últimos 60 s por este processo.

```json
{"group":"greenhouse-aggregator","group_state":"Stable","topic":"greenhouse.readings","total_lag":1520,
 "partitions":[{"partition":0,"committed_offset":10400,"end_offset":11000,"lag":600}],
 "dlq_messages":3,"throughput_records_per_second":79.8,"throughput_window_seconds":60}
```

`throughput_window_seconds` é o intervalo realmente medido (0 logo após a partida, quando a vazão ainda é 0).

## POST /dlq/replay

Reprocessa a DLQ: republica no tópico de leituras o que o agregador recusou, depois que a causa foi corrigida. Corpo opcional `{"limit":10,"reason":"unknown_schema","dry_run":true}`.

- `dry_run` ausente vale `true`: a resposta mostra o que seria republicado e nada é gravado. Só `"dry_run":false` republica.
- `limit` padrão 10, de 1 a `DLQ_REPLAY_MAX_BATCH` (padrão 100, teto 1000); acima disso, `400`.
- `reason` filtra por `malformed`, `unknown_schema` ou `invalid_reading`; outro valor é `400`.
- O destino vem de `DLQ_REPLAY_TOPIC` e não existe no corpo: um campo `destination` (ou qualquer campo desconhecido) é `400`. Só mensagens cujo header `dlq.source.topic` é o destino são elegíveis.
- **Leituras de janela já fechada não são republicadas pelo modo padrão** (use `"backfill":true`, abaixo). O agregador as descarta e conta como `late`, e republicar a leitura não as recupera. Por isso cada candidata é decodificada e comparada com o watermark do último checkpoint da partição de origem: se a janela já fechou, ela vai para `blocked` com `action: "would_be_late"`, não é republicada e não é marcada no ledger. Mensagem que ainda não decodifica vai para `blocked` como `undecodable` (voltaria à DLQ). O que fica em `selected` tem a janela aberta; o watermark pode andar entre a republicação e o consumo, então `replayed` significa "publicada", não "agregada".
- Cada mensagem da DLQ é republicada no máximo uma vez (ledger no Redis); repetir o pedido pega só o que sobrou. `409` se outra execução está em andamento.
- A mensagem republicada leva `dlq.replayed.by` (operador do token), `dlq.replay.run` e `dlq.replay.source.offset`; o ledger guarda operador, execução e horário.

```json
{"run_id":"9f2c1e0a4b7d3c55","dry_run":false,"operator":"ana","destination_topic":"greenhouse.readings",
 "scanned":3,"selected":[{"offset":0,"reason":"unknown_schema","action":"replayed"}],
 "blocked":[{"offset":1,"reason":"unknown_schema","action":"would_be_late"}],
 "replayed":1,"skipped":{"foreign_source":1,"would_be_late":1}}
```

### Backfill de janelas fechadas

`{"backfill":true,"limit":10,"dry_run":true}` recupera as leituras cuja janela já fechou: soma-as ao agregado já emitido da janela (lido do Postgres), republica o agregado recalculado em `greenhouse.aggregates` (o sink faz upsert) e reavalia o alerta. Idempotente por `event_id`; as mesmas regras de dry-run, lote, operador e prazo valem. Só janelas com fim há mais de `DLQ_BACKFILL_MIN_AGE` (5 min) entram; as mais novas ficam em `skipped.too_recent`. `invalid_reading` (valor impossível, relógio adiantado, campo vazio, validado com o instante de recepção original), `undecodable` e `base_unknown` (a janela não está no Postgres e não há prova de que o sink do conector passou do tópico) ficam em `blocked` e nunca são somadas. Leituras de janela ainda aberta ficam em `skipped.not_late` (use a republicação normal). A resposta traz `backfill.windows`, com `base_count`, `new_events`, `result_count`, `result_mean` e o `alert` que o novo agregado geraria. `501` se o backfill não está ligado (exige `POSTGRES_DSN`). Decisão no [ADR 0015](adr/0015-backfill-de-janelas-fechadas.md).

```json
{"run_id":"3b1f...","dry_run":true,"backfill":{"windows":[{"greenhouse_id":"gh-001","sensor_type":"TEMPERATURE",
  "window_start":"2026-03-10T12:00:00Z","new_events":2,"base_count":2,"result_count":4,"result_mean":38.5,"alert":"ABOVE_MAX"}],
  "republished_pending":0}}
```

A execução inteira tem um prazo de 60 s, menor que o TTL do lock (2 min, sem renovação). A rota só existe com `REDIS_ADDR` (o ledger). O mesmo caso de uso roda pela linha de comando: `telemetryctl dlq replay [-limit N] [-reason R] [-execute -operator NOME]`.

## Configuração

| Variável | Efeito |
| --- | --- |
| `REDIS_ADDR` | liga `/greenhouses` e `/greenhouses/{id}/state` |
| `OPERATOR_TOKENS` | `operador:token,...`; tokens de 8 ou mais caracteres. Sem ela, a API não é registrada |
| `POSTGRES_DSN` | liga séries e alertas; cria as colunas de reconhecimento (`acknowledged_at`, `acknowledged_by`, `note`) se faltarem |
| `KAFKA_BROKERS` | DLQ e saúde do pipeline |
| `DLQ_REPLAY_TOPIC` | destino do reprocessamento (padrão `greenhouse.readings`) |
| `WINDOW_SIZE` | tamanho da janela, usado pelo reprocessador para saber se a janela de uma leitura já fechou |
| `DLQ_BACKFILL_SINK_GROUP` | grupo do conector JDBC de agregados, usado como prova de que o sink alcançou o tópico (padrão `connect-greenhouse-aggregates-jdbc-sink`) |
| `DLQ_BACKFILL_MIN_AGE` | idade mínima do fim da janela para o backfill (padrão 5m) |
| `DLQ_REPLAY_MAX_BATCH` | teto de mensagens por execução (padrão 100, máximo 1000) |
| `THRESHOLD_*` | faixas agronômicas |
