# Observabilidade e métricas

## Endpoints

| Serviço | Métricas | Saúde |
| --- | --- | --- |
| Simulador | `http://localhost:9101/metrics` | `/healthz` |
| Agregador | `http://localhost:9102/metrics` | `/healthz` |
| Prometheus | `http://localhost:9090` (profile `observability`) | `/-/healthy` |
| Jaeger | `http://localhost:16686` (profile `observability`) | |

## Métricas do agregador

| Métrica | Tipo | Para que serve |
| --- | --- | --- |
| `telemetry_aggregator_consumer_lag{topic,partition}` | gauge | Registros entre o high watermark e a posição de consumo. Principal sinal de saturação. |
| `telemetry_aggregator_records_consumed_total` | counter | Throughput de consumo (`rate(...)`). |
| `telemetry_aggregator_records_total{outcome}` | counter | Destino de cada leitura: `accepted`, `duplicate`, `late`, `dead_lettered`. |
| `telemetry_aggregator_dead_letters_total{reason}` | counter | DLQ por motivo: `malformed`, `unknown_schema`, `invalid_reading`. |
| `telemetry_aggregator_aggregates_emitted_total` | counter | Janelas fechadas e publicadas. |
| `telemetry_aggregator_alerts_emitted_total` | counter | Alertas de limite. |
| `telemetry_aggregator_open_windows{topic,partition}` | gauge | Janelas em memória (custo de estado). |
| `telemetry_aggregator_dedup_tracked_events{topic,partition}` | gauge | Ids retidos pelo deduplicador. |
| `telemetry_aggregator_watermark_timestamp_seconds{topic,partition}` | gauge | `time() - watermark` mostra o atraso em tempo de evento. |
| `telemetry_aggregator_committed_offset{topic,partition}` | gauge | Progresso do commit. |
| `telemetry_aggregator_end_to_end_latency_seconds` | histogram | Do `measured_at` até o processamento. |
| `telemetry_aggregator_batch_duration_seconds` | histogram | Lote completo: processamento, publish, checkpoint e commit. |

## Métricas do simulador

| Métrica | Tipo |
| --- | --- |
| `telemetry_simulator_readings_produced_total{sensor_type}` | counter (só leituras confirmadas pelo broker) |
| `telemetry_simulator_produce_errors_total` | counter |
| `telemetry_simulator_produce_ack_latency_seconds` | histogram (acks=all) |

## Consultas úteis

```promql
# Throughput de consumo e de produção
rate(telemetry_aggregator_records_consumed_total[1m])
sum(rate(telemetry_simulator_readings_produced_total[1m]))

# Lag total e por partição
sum(telemetry_aggregator_consumer_lag)

# Atraso em tempo de evento (segundos)
time() - min(telemetry_aggregator_watermark_timestamp_seconds)

# Latência fim a fim p99
histogram_quantile(0.99, rate(telemetry_aggregator_end_to_end_latency_seconds_bucket[5m]))

# Taxa de DLQ
sum(rate(telemetry_aggregator_dead_letters_total[5m])) / sum(rate(telemetry_aggregator_records_consumed_total[5m]))
```

Alertas versionados em `deploy/prometheus/alerts.yml`: `AggregatorLagHigh`, `AggregatorStalled` e `DeadLetterRateHigh`.

## Logs

JSON no stdout via `slog`, sempre com `service`. Eventos com contexto de partição e offset: envio para a DLQ (motivo e erro), alerta emitido, partição atribuída (com o checkpoint restaurado) e erros retentáveis.

## Traces

Com `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318` e o profile `observability`, o plugin `kotel` do franz-go cria spans de produce e de consumo e propaga o contexto W3C (`traceparent`) nos headers das mensagens. A amostragem segue `OTEL_TRACES_SAMPLER`/`OTEL_TRACES_SAMPLER_ARG` (padrão: 5% com respeito ao pai), porque cada leitura geraria dois spans.

## Métricas de entrega (Tech Metrics)

Além das métricas do sistema, o repositório acompanha as quatro métricas DORA a partir do histórico do GitHub, para medir o fluxo de entrega do próprio pipeline:

| Métrica | Como medir aqui | Meta inicial |
| --- | --- | --- |
| Frequência de deploy | Merges na `main` com CI verde por semana | ≥ 2 |
| Lead time de mudança | Do primeiro commit da branch ao merge | < 2 dias |
| Taxa de falha de mudança | Reverts ou hotfixes / merges | < 15% |
| Tempo de restauração | Da abertura de issue `bug` com impacto em garantia até o fix na `main` | < 1 dia |

E SLIs do pipeline que viram SLOs no plano de evolução: lag p99 < 30 s, latência fim a fim p99 < 90 s (inclui a janela de 1 min) e taxa de DLQ < 1%.
