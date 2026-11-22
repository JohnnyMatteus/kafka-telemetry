package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/history"
)

// maxPoints protege a API de um intervalo grande demais.
const maxPoints = 20000

// Series implementa history.Repository.
type Series struct{ store *Store }

// Series devolve o repositório de séries.
func (s *Store) Series() *Series { return &Series{store: s} }

// Series lê os agregados com início em [from, to), por tipo, em ordem de tempo.
func (r *Series) Series(ctx context.Context, greenhouseID string, from, to time.Time) (map[string]history.Series, error) {
	rows, err := r.store.pool.Query(ctx, `
SELECT sensor_type, unit, window_start, window_end, sample_count, mean_value, min_value, max_value
FROM greenhouse_window_aggregates
WHERE greenhouse_id = $1 AND window_start >= $2 AND window_start < $3
ORDER BY sensor_type, window_start
LIMIT $4`, greenhouseID, from, to, maxPoints)
	if err != nil {
		return nil, fmt.Errorf("consultar série: %w", err)
	}
	defer rows.Close()
	out := map[string]history.Series{}
	for rows.Next() {
		var typ, unit string
		var p history.Point
		if err := rows.Scan(&typ, &unit, &p.WindowStart, &p.WindowEnd, &p.SampleCount, &p.Mean, &p.Min, &p.Max); err != nil {
			return nil, fmt.Errorf("ler ponto: %w", err)
		}
		p.WindowStart, p.WindowEnd = p.WindowStart.UTC(), p.WindowEnd.UTC()
		s := out[typ]
		s.Unit = unit
		s.Points = append(s.Points, p)
		out[typ] = s
	}
	return out, rows.Err()
}
