package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/alerting"
)

const alertColumns = `alert_id, greenhouse_id, sensor_type, window_start, window_end, rule,
	observed_value, limit_value, acknowledged_at, note`

// Alerts implementa alerting.Repository.
type Alerts struct{ store *Store }

// Alerts devolve o repositório de alertas.
func (s *Store) Alerts() *Alerts { return &Alerts{store: s} }

func scanAlert(row pgx.Row) (alerting.Alert, error) {
	var a alerting.Alert
	err := row.Scan(&a.ID, &a.GreenhouseID, &a.SensorType, &a.WindowStart, &a.WindowEnd, &a.Rule,
		&a.ObservedValue, &a.LimitValue, &a.AcknowledgedAt, &a.Note)
	a.WindowStart, a.WindowEnd = a.WindowStart.UTC(), a.WindowEnd.UTC()
	if a.AcknowledgedAt != nil {
		t := a.AcknowledgedAt.UTC()
		a.AcknowledgedAt = &t
	}
	return a, err
}

// List devolve os alertas mais recentes primeiro.
func (r *Alerts) List(ctx context.Context, status alerting.Status, limit int) ([]alerting.Alert, error) {
	filter := ""
	switch status {
	case alerting.StatusOpen:
		filter = "WHERE acknowledged_at IS NULL"
	case alerting.StatusAcknowledged:
		filter = "WHERE acknowledged_at IS NOT NULL"
	}
	rows, err := r.store.pool.Query(ctx,
		`SELECT `+alertColumns+` FROM greenhouse_alerts `+filter+` ORDER BY window_start DESC, alert_id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("listar alertas: %w", err)
	}
	defer rows.Close()
	out := []alerting.Alert{}
	for rows.Next() {
		a, err := scanAlert(rows)
		if err != nil {
			return nil, fmt.Errorf("ler alerta: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// Acknowledge reconhece de forma atômica: o UPDATE condicional impede que dois
// operadores reconheçam o mesmo alerta; quem perde descobre o motivo depois.
func (r *Alerts) Acknowledge(ctx context.Context, id, note string, at time.Time) (alerting.Alert, error) {
	var storedNote *string
	if note != "" {
		storedNote = &note
	}
	a, err := scanAlert(r.store.pool.QueryRow(ctx,
		`UPDATE greenhouse_alerts SET acknowledged_at = $2, note = $3
		 WHERE alert_id = $1 AND acknowledged_at IS NULL RETURNING `+alertColumns, id, at, storedNote))
	if err == nil {
		return a, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return alerting.Alert{}, fmt.Errorf("reconhecer alerta: %w", err)
	}
	var exists bool
	if err := r.store.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM greenhouse_alerts WHERE alert_id = $1)`, id).Scan(&exists); err != nil {
		return alerting.Alert{}, fmt.Errorf("conferir alerta: %w", err)
	}
	if !exists {
		return alerting.Alert{}, alerting.ErrNotFound
	}
	return alerting.Alert{}, alerting.ErrAlreadyAcknowledged
}
