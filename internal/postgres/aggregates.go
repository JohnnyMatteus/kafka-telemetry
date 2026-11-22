package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/dlq"
)

// Aggregates implementa dlq.AggregateReader: lê o agregado já emitido de uma janela.
type Aggregates struct{ store *Store }

// Aggregates devolve o leitor de agregados.
func (s *Store) Aggregates() *Aggregates { return &Aggregates{store: s} }

// Window devolve a base da janela; Found é falso se o sink nunca a gravou.
func (a *Aggregates) Window(ctx context.Context, greenhouseID, sensorType string, start time.Time) (dlq.Base, error) {
	var n int
	var mean, lo, hi float64
	err := a.store.pool.QueryRow(ctx, `
SELECT sample_count, mean_value, min_value, max_value
FROM greenhouse_window_aggregates
WHERE greenhouse_id = $1 AND sensor_type = $2 AND window_start = $3`, greenhouseID, sensorType, start).Scan(&n, &mean, &lo, &hi)
	if errors.Is(err, pgx.ErrNoRows) {
		return dlq.Base{}, nil
	}
	if err != nil {
		return dlq.Base{}, fmt.Errorf("ler a janela: %w", err)
	}
	return dlq.Base{Found: true, Count: n, Sum: mean * float64(n), Min: lo, Max: hi}, nil
}
