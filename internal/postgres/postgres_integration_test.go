//go:build integration

// Rode com: go test -tags integration ./internal/postgres/...
// Sem Docker disponível, os testes são pulados.
package postgres_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/alerting"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/postgres"
)

var t0 = time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

func start(t *testing.T) *postgres.Store {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	initSQL, err := filepath.Abs("../../deploy/postgres/init.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(initSQL); err != nil {
		t.Fatal(err)
	}
	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("telemetry"), tcpostgres.WithUsername("telemetry"), tcpostgres.WithPassword("test"),
		tcpostgres.WithInitScripts(initSQL), tcpostgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	store, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store
}

func TestAlertsAckLifecycle(t *testing.T) {
	store := start(t)
	ctx := context.Background()
	// Simula o sink JDBC: só as colunas do registro.
	if err := store.Exec(ctx, `INSERT INTO greenhouse_alerts
		(alert_id, greenhouse_id, sensor_type, window_start, window_end, rule, observed_value, limit_value)
		VALUES ('a1','gh-001','TEMPERATURE',$1,$2,'ABOVE_MAX',36,32), ('a2','gh-001','HUMIDITY',$1,$2,'BELOW_MIN',40,50)`,
		t0, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	repo := store.Alerts()

	open, err := repo.List(ctx, alerting.StatusOpen, 10)
	if err != nil || len(open) != 2 {
		t.Fatalf("abertos = %d, err=%v", len(open), err)
	}
	acked, err := repo.Acknowledge(ctx, "a1", "ana", "ventilação ligada", t0.Add(time.Hour))
	if err != nil || acked.AcknowledgedAt == nil || acked.Note == nil || *acked.Note != "ventilação ligada" {
		t.Fatalf("ack = %+v, err=%v", acked, err)
	}
	if _, err := repo.Acknowledge(ctx, "a1", "bruno", "", t0); !errors.Is(err, alerting.ErrAlreadyAcknowledged) {
		t.Fatalf("segundo ack deveria dar ErrAlreadyAcknowledged, veio %v", err)
	}
	if _, err := repo.Acknowledge(ctx, "nope", "ana", "", t0); !errors.Is(err, alerting.ErrNotFound) {
		t.Fatalf("inexistente deveria dar ErrNotFound, veio %v", err)
	}

	// Um upsert do sink (replay do mesmo alerta) não pode apagar o reconhecimento.
	if err := store.Exec(ctx, `INSERT INTO greenhouse_alerts
		(alert_id, greenhouse_id, sensor_type, window_start, window_end, rule, observed_value, limit_value)
		VALUES ('a1','gh-001','TEMPERATURE',$1,$2,'ABOVE_MAX',36,32)
		ON CONFLICT (alert_id) DO UPDATE SET observed_value = EXCLUDED.observed_value`, t0, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	done, _ := repo.List(ctx, alerting.StatusAcknowledged, 10)
	if len(done) != 1 || done[0].ID != "a1" {
		t.Fatalf("reconhecidos = %+v", done)
	}
}

func TestSeriesGroupsByTypeInRange(t *testing.T) {
	store := start(t)
	ctx := context.Background()
	for i := range 3 {
		start := t0.Add(time.Duration(i) * time.Minute)
		if err := store.Exec(ctx, `INSERT INTO greenhouse_window_aggregates
			(greenhouse_id, sensor_type, window_start, window_end, sample_count, min_value, max_value, mean_value, unit)
			VALUES ('gh-001','TEMPERATURE',$1,$2,3,19,21,20,'celsius')`, start, start.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.Series().Series(ctx, "gh-001", t0, t0.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	s := got["TEMPERATURE"]
	if s.Unit != "celsius" || len(s.Points) != 2 || !s.Points[0].WindowStart.Equal(t0) {
		t.Fatalf("série = %+v (intervalo é [from, to))", s)
	}
}

// O backfill da DLQ parte do agregado que o sink JDBC gravou.
func TestAggregatesWindowReadsTheEmittedAggregate(t *testing.T) {
	store := start(t)
	ctx := context.Background()
	if err := store.Exec(ctx, `INSERT INTO greenhouse_window_aggregates
		(greenhouse_id, sensor_type, window_start, window_end, sample_count, min_value, max_value, mean_value, unit)
		VALUES ('gh-001','TEMPERATURE',$1,$2,4,18,26,22,'celsius')`, t0, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err := store.Aggregates().Window(ctx, "gh-001", "TEMPERATURE", t0)
	if err != nil || !got.Found || got.Count != 4 || got.Sum != 88 || got.Min != 18 || got.Max != 26 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	missing, err := store.Aggregates().Window(ctx, "gh-001", "TEMPERATURE", t0.Add(time.Hour))
	if err != nil || missing.Found {
		t.Fatalf("missing=%+v err=%v", missing, err)
	}
}
