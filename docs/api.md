# API de consulta

JSON em `snake_case`, servida pelo agregador na porta de HTTP (`METRICS_ADDR`, padrão `:9102`; no compose, `AGGREGATOR_METRICS_PORT`). Decisão em [ADR 0010](adr/0010-api-de-consulta-no-agregador.md). Horários em RFC3339 UTC.

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

`rule` é `ABOVE_MAX` ou `BELOW_MIN`. `acknowledged_at` e `note` são sempre presentes, `null` quando ausentes.

## POST /alerts/{id}/ack

Corpo `{"note":"texto"}` (a nota é opcional, até 500 caracteres). Devolve o alerta atualizado, no formato acima. `404` se o alerta não existe e `409` se já foi reconhecido; a operação é atômica, então dois operadores simultâneos recebem um `200` e um `409`.

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

## Configuração

| Variável | Efeito |
| --- | --- |
| `REDIS_ADDR` | liga `/greenhouses` e `/greenhouses/{id}/state` |
| `POSTGRES_DSN` | liga séries e alertas; cria as colunas de reconhecimento se faltarem |
| `KAFKA_BROKERS` | DLQ e saúde do pipeline |
| `THRESHOLD_*` | faixas agronômicas |
