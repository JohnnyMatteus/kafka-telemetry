package simulator_test

import (
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/simulator"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

var now = time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)

func generate(t *testing.T, cfg simulator.Config, n int) []telemetry.Reading {
	t.Helper()
	g, err := simulator.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]telemetry.Reading, n)
	for i := range out {
		out[i] = g.Next(now)
	}
	return out
}

func TestGeneratorProducesValidReadingsForEverySensor(t *testing.T) {
	readings := generate(t, simulator.Config{Greenhouses: 3, Seed: 1}, 12)
	seen := map[string]bool{}
	uuid := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	for _, r := range readings {
		if err := r.Validate(now); err != nil {
			t.Fatalf("leitura inválida sem InvalidRatio: %v", err)
		}
		if !uuid.MatchString(r.EventID) {
			t.Fatalf("event_id %q não é UUID v4", r.EventID)
		}
		seen[r.SensorID] = true
	}
	if len(seen) != 12 {
		t.Fatalf("esperava 12 sensores distintos (3 estufas x 4 tipos), vieram %d", len(seen))
	}
}

func TestGeneratorIsDeterministicForSeed(t *testing.T) {
	cfg := simulator.Config{Greenhouses: 2, DuplicateRatio: 0.2, InvalidRatio: 0.1, Seed: 42}
	a, b := generate(t, cfg, 50), generate(t, cfg, 50)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("posição %d diverge: %+v x %+v", i, a[i], b[i])
		}
	}
}

func TestGeneratorEmitsDuplicatesAndInvalidReadings(t *testing.T) {
	readings := generate(t, simulator.Config{Greenhouses: 5, DuplicateRatio: 0.1, InvalidRatio: 0.05, Seed: 7}, 2000)
	ids := map[string]int{}
	invalid := 0
	for _, r := range readings {
		ids[r.EventID]++
		if r.Validate(now) != nil {
			invalid++
		}
	}
	duplicates := len(readings) - len(ids)
	if duplicates < 100 || duplicates > 300 {
		t.Errorf("duplicatas = %d, esperava em torno de 10%%", duplicates)
	}
	if invalid < 50 || invalid > 200 {
		t.Errorf("inválidas = %d, esperava em torno de 5%%", invalid)
	}
}

func TestAnomalousGreenhouseRunsHot(t *testing.T) {
	readings := generate(t, simulator.Config{Greenhouses: 4, AnomalyRatio: 0.25, Seed: 3}, 400)
	limit := telemetry.DefaultThresholds()[telemetry.SensorTemperature].Max
	for _, r := range readings {
		if r.Type != telemetry.SensorTemperature {
			continue
		}
		hot := r.GreenhouseID == simulator.GreenhouseID(0)
		if hot && r.Value <= limit {
			t.Fatalf("estufa anômala com %.2f, deveria passar de %.0f", r.Value, limit)
		}
		if !hot && r.Value > limit {
			t.Fatalf("estufa normal %s com %.2f acima do limite", r.GreenhouseID, r.Value)
		}
	}
}

func TestConfigValidation(t *testing.T) {
	for _, cfg := range []simulator.Config{
		{Greenhouses: 0},
		{Greenhouses: 1, DuplicateRatio: -0.1},
		{Greenhouses: 1, InvalidRatio: 1.5},
	} {
		if _, err := simulator.New(cfg); !errors.Is(err, simulator.ErrInvalidConfig) {
			t.Errorf("%+v: esperava ErrInvalidConfig, veio %v", cfg, err)
		}
	}
}
