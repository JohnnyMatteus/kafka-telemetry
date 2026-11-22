# Observabilidade e métricas

## Endpoints

| Serviço | Métricas | Saúde |
| --- | --- | --- |
| Simulador | `http://localhost:9101/metrics` | `/healthz` |
| Agregador | `http://localhost:9102/metrics` | `/healthz` |
| Notificador | `http://127.0.0.1:9103/metrics` | `/healthz` |
| Prometheus | `http://localhost:9090` (profile `observability`) | `/-/healthy` |
| Grafana | `http://127.0.0.1:3000` (profile `observability`; usuário `admin`, senha `GRAFANA_ADMIN_PASSWORD`, padrão `dev-only-change-me`) | `/api/health` |
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
| `telemetry_aggregator_idle_windows_closed_total` | counter | Janelas fechadas pelo watermark ocioso ([ADR 0014](adr/0014-watermark-por-estufa-e-ocioso.md)). |
| `telemetry_aggregator_state_updates_total{result}` | counter | Gravações do último estado no Redis, por resultado. |

## Métricas do simulador

| Métrica | Tipo |
| --- | --- |
| `telemetry_simulator_readings_produced_total{sensor_type}` | counter (só leituras confirmadas pelo broker) |
| `telemetry_simulator_produce_errors_total` | counter |
| `telemetry_simulator_produce_ack_latency_seconds` | histogram (acks=all) |

## Métricas do notificador

| Métrica | Tipo | Para que serve |
| --- | --- | --- |
| `telemetry_notifier_notifications_total{channel,outcome}` | counter | Avisos por canal e resultado: `sent`, `duplicate`, `queued`, `failed`, `expired`, `permanent_failure`, `no_recipient`, `undecodable`. |
| `telemetry_notifier_retry_queue_depth` | gauge | Avisos esperando nova tentativa. |
| `telemetry_notifier_retry_oldest_age_seconds` | gauge | Idade do aviso mais antigo na fila. |
| `telemetry_notifier_alerts_consumed_total`, `telemetry_notifier_consumer_lag`, `telemetry_notifier_batch_duration_seconds` | counter, gauge, histogram | Consumo do tópico de alertas. |

## Dashboards (Grafana)

Versionados em `deploy/grafana/dashboards` e provisionados sem edição pela interface (`deploy/grafana/provisioning`): **Telemetria: pipeline** (vazão, lag, latência, destino das leituras, DLQ, estado em memória), **Telemetria: SLOs** (orçamento de erro e burn rate) e **Telemetria: notificador** (avisos, fila de retry). Suba com `make up` (o profile `observability` inclui o Grafana). O teste `internal/hygiene` falha se um dashboard ou uma regra citar uma métrica que nenhum código registra, se uma série `slo:*` do dashboard não tiver regra de gravação, ou se o bucket do SLO de latência não existir no histograma.

## SLOs

Regras de gravação e alertas em `deploy/prometheus/slo.yml`, com alertas de **burn rate em duas janelas** (rápido: 5 min e 1 h acima de 14,4 vezes, crítico; lento: 30 min e 6 h acima de 6 vezes, aviso), sobre um orçamento de 1% em 30 dias.

| SLO | SLI | Meta | Alertas |
| --- | --- | --- | --- |
| Lag | `slo:consumer_lag_seconds` (lag total dividido pela vazão de consumo dos últimos 5 min) abaixo de 30 s, medido por amostra (`slo:consumer_lag_ok`) | 99% do tempo | `SLOLagBurnFast`, `SLOLagBurnSlow` |
| Latência | fração das leituras processadas em até 65,5 s da medição (`slo:latency_good_ratio:rate*`) | 99% | `SLOLatencyBurnFast`, `SLOLatencyBurnSlow` |
| DLQ | `slo:dlq_ratio:rate5m` abaixo de 1% | 1% | `SLODeadLetterRate` (15 min) |

Limites, para não confiar no que o SLO não mede:

- A meta de latência de 90 s do plano antigo incluía a espera da janela; o histograma mede cada leitura da medição ao processamento e **não** inclui essa espera. A meta de 65,5 s é o maior bucket abaixo de 90 s. O atraso até o **agregado** sair (janela mais tolerância, cerca de 70 s) não tem métrica e não está no SLO.
- O lag em segundos é uma estimativa (registros em atraso pela vazão recente). O gauge de lag só é atualizado quando um lote chega; um consumidor parado não o atualiza, então `AggregatorStalled` continua sendo o alerta para esse caso.
- Sem tráfego, a razão de latência é indefinida (0/0) e os alertas de latência não disparam; a vazão mínima de 1 registro/s mantém o lag zero dentro da meta.
- As regras foram escritas e verificadas só contra os nomes de métrica e de bucket do código. **A sintaxe PromQL é validada pelo `promtool` no CI** (job `prometheus-rules`); não foi executada localmente.

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

Alertas versionados em `deploy/prometheus/alerts.yml`: `AggregatorLagHigh`, `AggregatorStalled`, `DeadLetterRateHigh`, `NotifierRetryQueueStale` e `NotifierDeliveryExpired`; os de SLO ficam em `slo.yml`.

## Logs

JSON no stdout via `slog`, sempre com `service`. Eventos com contexto de partição e offset: envio para a DLQ (motivo e erro), alerta emitido, partição atribuída (com o checkpoint restaurado) e erros retentáveis.

## Traces

Com `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318` e o profile `observability`, o plugin `kotel` do franz-go cria spans de produce e de consumo e propaga o contexto W3C (`traceparent`) nos headers das mensagens. A amostragem segue `OTEL_TRACES_SAMPLER`/`OTEL_TRACES_SAMPLER_ARG` (padrão: 5% com respeito ao pai), porque cada leitura geraria dois spans.

## Métricas de entrega (Tech Metrics)

O fluxo de entrega do repositório tem estas **metas**, acompanhadas à mão a partir do histórico do GitHub. **Não há automação que as meça**: nenhum script, painel ou job calcula frequência, lead time, falha ou restauração; as metas só valem se alguém olhar.

| Métrica | Como medir | Meta inicial |
| --- | --- | --- |
| Frequência de deploy | Merges na `main` com CI verde por semana | ≥ 2 |
| Lead time de mudança | Do primeiro commit da branch ao merge | < 2 dias |
| Taxa de falha de mudança | Reverts ou hotfixes / merges | < 15% |
| Tempo de restauração | Da abertura de issue `bug` com impacto em garantia até o fix na `main` | < 1 dia |

Os SLOs do pipeline (lag, latência e DLQ) estão definidos acima.
