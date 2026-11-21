package state

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
)

// Instrumented decora um aggregation.StateStore com a contagem de
// atualizações por resultado, sem que o adaptador conheça o Prometheus.
type Instrumented struct {
	next    aggregation.StateStore
	updates *prometheus.CounterVec
}

// NewInstrumented registra a métrica e envolve o store.
func NewInstrumented(next aggregation.StateStore, reg prometheus.Registerer) *Instrumented {
	updates := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "telemetry_aggregator_state_updates_total",
		Help: "Estados de estufa gravados na projeção de último estado, por resultado.",
	}, []string{"result"})
	reg.MustRegister(updates)
	return &Instrumented{next: next, updates: updates}
}

// Update delega e conta.
func (i *Instrumented) Update(ctx context.Context, states []aggregation.SensorState) error {
	err := i.next.Update(ctx, states)
	result := "ok"
	if err != nil {
		result = "error"
	}
	i.updates.WithLabelValues(result).Add(float64(len(states)))
	return err
}
