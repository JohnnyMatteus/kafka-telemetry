// Package observability reúne logs estruturados, métricas Prometheus e
// tracing OpenTelemetry dos binários do pipeline.
package observability

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/notify"
)

// latencyBuckets cobre de 1 ms a ~2 min: o fim a fim inclui a janela de 1 minuto.
var latencyBuckets = prometheus.ExponentialBuckets(0.001, 2, 18)

// AggregatorMetrics implementa aggregation.Observer e kafka.LoopObserver.
type AggregatorMetrics struct {
	records    *prometheus.CounterVec
	deadLetter *prometheus.CounterVec
	aggregates prometheus.Counter
	alerts     prometheus.Counter
	consumed   prometheus.Counter
	consumedN  atomic.Int64
	lag        *prometheus.GaugeVec
	openWin    *prometheus.GaugeVec
	tracked    *prometheus.GaugeVec
	watermark  *prometheus.GaugeVec
	committed  *prometheus.GaugeVec
	endToEnd   prometheus.Histogram
	batch      prometheus.Histogram
}

// NewAggregatorMetrics registra as métricas do agregador.
func NewAggregatorMetrics(reg prometheus.Registerer) *AggregatorMetrics {
	tp := []string{"topic", "partition"}
	m := &AggregatorMetrics{
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "telemetry_aggregator_records_total", Help: "Leituras processadas por destino (accepted, duplicate, late, dead_lettered).",
		}, []string{"outcome"}),
		deadLetter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "telemetry_aggregator_dead_letters_total", Help: "Mensagens enviadas para a DLQ por motivo.",
		}, []string{"reason"}),
		aggregates: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "telemetry_aggregator_aggregates_emitted_total", Help: "Janelas fechadas e publicadas.",
		}),
		alerts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "telemetry_aggregator_alerts_emitted_total", Help: "Alertas de limite publicados.",
		}),
		consumed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "telemetry_aggregator_records_consumed_total", Help: "Registros lidos do tópico de leituras (throughput).",
		}),
		lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "telemetry_aggregator_consumer_lag", Help: "Registros entre o high watermark e a posição de consumo.",
		}, tp),
		openWin: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "telemetry_aggregator_open_windows", Help: "Janelas abertas em memória.",
		}, tp),
		tracked: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "telemetry_aggregator_dedup_tracked_events", Help: "Ids de evento retidos pelo deduplicador.",
		}, tp),
		watermark: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "telemetry_aggregator_watermark_timestamp_seconds", Help: "Watermark de tempo de evento da partição.",
		}, tp),
		committed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "telemetry_aggregator_committed_offset", Help: "Último offset commitado.",
		}, tp),
		endToEnd: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "telemetry_aggregator_end_to_end_latency_seconds", Help: "Do horário da medição até o processamento.", Buckets: latencyBuckets,
		}),
		batch: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "telemetry_aggregator_batch_duration_seconds", Help: "Duração do processamento de um lote, incluindo publish e commit.", Buckets: latencyBuckets,
		}),
	}
	reg.MustRegister(m.records, m.deadLetter, m.aggregates, m.alerts, m.consumed, m.lag, m.openWin, m.tracked, m.watermark, m.committed, m.endToEnd, m.batch)
	return m
}

func labels(tp aggregation.TopicPartition) prometheus.Labels {
	return prometheus.Labels{"topic": tp.Topic, "partition": strconv.Itoa(int(tp.Partition))}
}

func (m *AggregatorMetrics) RecordProcessed(outcome string) { m.records.WithLabelValues(outcome).Inc() }
func (m *AggregatorMetrics) DeadLettered(reason string)     { m.deadLetter.WithLabelValues(reason).Inc() }
func (m *AggregatorMetrics) Emitted(aggregates, alerts int) {
	m.aggregates.Add(float64(aggregates))
	m.alerts.Add(float64(alerts))
}
func (m *AggregatorMetrics) EndToEndLatency(d time.Duration) { m.endToEnd.Observe(d.Seconds()) }
func (m *AggregatorMetrics) PartitionState(tp aggregation.TopicPartition, open, tracked int, wm time.Time) {
	l := labels(tp)
	m.openWin.With(l).Set(float64(open))
	m.tracked.With(l).Set(float64(tracked))
	if !wm.IsZero() {
		m.watermark.With(l).Set(float64(wm.UnixMilli()) / 1000)
	}
}
func (m *AggregatorMetrics) Committed(tp aggregation.TopicPartition, offset int64) {
	m.committed.With(labels(tp)).Set(float64(offset))
}
func (m *AggregatorMetrics) Lag(tp aggregation.TopicPartition, lag int64) {
	m.lag.With(labels(tp)).Set(float64(lag))
}
func (m *AggregatorMetrics) Consumed(n int) {
	m.consumed.Add(float64(n))
	m.consumedN.Add(int64(n))
}

// ConsumedTotal é o total lido desde o início do processo, para a API de saúde.
func (m *AggregatorMetrics) ConsumedTotal() int64          { return m.consumedN.Load() }
func (m *AggregatorMetrics) BatchDuration(d time.Duration) { m.batch.Observe(d.Seconds()) }

// SimulatorMetrics são as métricas do produtor.
type SimulatorMetrics struct {
	Produced *prometheus.CounterVec
	Errors   prometheus.Counter
	Latency  prometheus.Histogram
}

// NewSimulatorMetrics registra as métricas do simulador.
func NewSimulatorMetrics(reg prometheus.Registerer) *SimulatorMetrics {
	m := &SimulatorMetrics{
		Produced: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "telemetry_simulator_readings_produced_total", Help: "Leituras confirmadas pelo broker, por tipo de sensor.",
		}, []string{"sensor_type"}),
		Errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "telemetry_simulator_produce_errors_total", Help: "Leituras que falharam definitivamente no produce.",
		}),
		Latency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "telemetry_simulator_produce_ack_latency_seconds", Help: "Do envio até o ack (acks=all).", Buckets: latencyBuckets,
		}),
	}
	reg.MustRegister(m.Produced, m.Errors, m.Latency)
	return m
}

// NotifierMetrics implementa notify.Observer e kafka.LoopObserver.
type NotifierMetrics struct {
	notifications *prometheus.CounterVec
	consumed      prometheus.Counter
	lag           *prometheus.GaugeVec
	batch         prometheus.Histogram
	retryDepth    prometheus.Gauge
	retryOldest   prometheus.Gauge
}

// NewNotifierMetrics registra as métricas do notificador.
func NewNotifierMetrics(reg prometheus.Registerer) *NotifierMetrics {
	m := &NotifierMetrics{
		notifications: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "telemetry_notifier_notifications_total",
			Help: "Avisos por canal e resultado (sent, duplicate, queued, failed, expired, permanent_failure, no_recipient, undecodable).",
		}, []string{"channel", "outcome"}),
		consumed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "telemetry_notifier_alerts_consumed_total", Help: "Alertas lidos do tópico.",
		}),
		lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "telemetry_notifier_consumer_lag", Help: "Alertas entre o high watermark e a posição de consumo.",
		}, []string{"topic", "partition"}),
		batch: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "telemetry_notifier_batch_duration_seconds", Help: "Duração de um lote, incluindo os avisos e o commit.", Buckets: latencyBuckets,
		}),
		retryDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "telemetry_notifier_retry_queue_depth", Help: "Avisos esperando nova tentativa.",
		}),
		retryOldest: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "telemetry_notifier_retry_oldest_age_seconds", Help: "Idade do aviso mais antigo na fila de retry.",
		}),
	}
	reg.MustRegister(m.notifications, m.consumed, m.lag, m.batch, m.retryDepth, m.retryOldest)
	return m
}

func (m *NotifierMetrics) Notified(channel, outcome string) {
	m.notifications.WithLabelValues(channel, outcome).Inc()
}
func (m *NotifierMetrics) Lag(tp aggregation.TopicPartition, lag int64) {
	m.lag.With(labels(tp)).Set(float64(lag))
}
func (m *NotifierMetrics) Consumed(n int)                { m.consumed.Add(float64(n)) }
func (m *NotifierMetrics) BatchDuration(d time.Duration) { m.batch.Observe(d.Seconds()) }

// QueueStats implementa notify.StatsObserver.
func (m *NotifierMetrics) QueueStats(st notify.QueueStats) {
	m.retryDepth.Set(float64(st.Depth))
	m.retryOldest.Set(st.OldestAge.Seconds())
}
