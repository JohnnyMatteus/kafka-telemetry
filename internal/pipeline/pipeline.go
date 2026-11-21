// Package pipeline modela o que se observa do pipeline Kafka: mensagens
// mortas na DLQ e a saúde do consumo.
package pipeline

import (
	"context"
	"time"
)

// DeadLetter é uma mensagem da DLQ com o motivo registrado pelo agregador.
type DeadLetter struct {
	Partition       int32
	Offset          int64
	Timestamp       time.Time
	Key             string
	Reason          string
	Error           string
	SourceTopic     string
	SourcePartition int32 // -1 quando o header não existe
	SourceOffset    int64
}

// DLQReader lê as mensagens mais recentes da DLQ, da mais nova para a mais antiga.
type DLQReader interface {
	Recent(ctx context.Context, limit int) ([]DeadLetter, error)
}

// PartitionLag é o atraso do grupo em uma partição.
type PartitionLag struct {
	Partition       int32 `json:"partition"`
	CommittedOffset int64 `json:"committed_offset"`
	EndOffset       int64 `json:"end_offset"`
	Lag             int64 `json:"lag"`
}

// Health é o retrato do consumo do grupo agregador.
type Health struct {
	Group           string         `json:"group"`
	GroupState      string         `json:"group_state"`
	Topic           string         `json:"topic"`
	TotalLag        int64          `json:"total_lag"`
	Partitions      []PartitionLag `json:"partitions"`
	DLQMessages     int64          `json:"dlq_messages"`
	ThroughputPerS  float64        `json:"throughput_records_per_second"`
	ThroughputSince int            `json:"throughput_window_seconds"`
}

// HealthReporter mede lag e fila morta no broker.
type HealthReporter interface {
	Report(ctx context.Context) (Health, error)
}

// Throughput informa o ritmo de consumo observado pelo processo.
type Throughput interface {
	PerSecond() (rate float64, windowSeconds int)
}
