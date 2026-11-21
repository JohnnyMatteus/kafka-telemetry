package state_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/aggregation"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/state"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

var t0 = time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

func newStore(t *testing.T) (*state.RedisStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return state.NewRedisStore(client), mr
}

func temperature(start time.Time, mean float64) aggregation.SensorState {
	return aggregation.SensorState{Aggregate: telemetry.Aggregate{
		Key:   telemetry.WindowKey{GreenhouseID: "gh-001", Type: telemetry.SensorTemperature, Start: start},
		End:   start.Add(time.Minute),
		Count: 2, Min: mean - 1, Max: mean + 1, Mean: mean,
	}}
}

func TestUpdateKeepsNewestWindow(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	newer := temperature(t0.Add(time.Minute), 36)
	newer.Alert = &telemetry.Alert{Rule: telemetry.RuleAboveMax, Observed: 36, Limit: 32}

	if err := store.Update(ctx, []aggregation.SensorState{newer}); err != nil {
		t.Fatal(err)
	}
	// Replay de uma janela anterior não pode regredir o estado.
	if err := store.Update(ctx, []aggregation.SensorState{temperature(t0, 20)}); err != nil {
		t.Fatal(err)
	}
	gh, err := store.Get(ctx, "gh-001")
	if err != nil {
		t.Fatal(err)
	}
	got := gh.Sensors["TEMPERATURE"]
	if got.Mean != 36 || !got.WindowStart.Equal(t0.Add(time.Minute)) || got.Unit != "celsius" {
		t.Fatalf("estado = %+v, quero a janela mais nova", got)
	}
	if got.Alert == nil || got.Alert.Rule != "ABOVE_MAX" {
		t.Fatalf("alerta = %+v", got.Alert)
	}
}

func TestGetUnknownGreenhouse(t *testing.T) {
	store, _ := newStore(t)
	if _, err := store.Get(context.Background(), "gh-x"); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("err = %v, quero ErrNotFound", err)
	}
}

func TestHandler(t *testing.T) {
	store, _ := newStore(t)
	if err := store.Update(context.Background(), []aggregation.SensorState{temperature(t0, 22)}); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /greenhouses/{id}/state", state.Handler(store, slog.New(slog.DiscardHandler)))

	for path, want := range map[string]int{
		"/greenhouses/gh-001/state": http.StatusOK,
		"/greenhouses/gh-404/state": http.StatusNotFound,
		"/greenhouses/gh%20x/state": http.StatusBadRequest,
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != want {
			t.Errorf("%s: status %d, quero %d", path, rec.Code, want)
		}
		if want == http.StatusOK {
			var gh state.Greenhouse
			if err := json.Unmarshal(rec.Body.Bytes(), &gh); err != nil || gh.Sensors["TEMPERATURE"].Mean != 22 {
				t.Errorf("corpo = %s (%v)", rec.Body.String(), err)
			}
		}
	}
}

func TestUpdateReportsRedisDown(t *testing.T) {
	store, mr := newStore(t)
	mr.Close()
	if err := store.Update(context.Background(), []aggregation.SensorState{temperature(t0, 22)}); err == nil {
		t.Fatal("esperava erro com o redis fora do ar")
	}
}

func TestListReturnsAllGreenhousesSorted(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	other := temperature(t0, 21)
	other.Aggregate.Key.GreenhouseID = "gh-000"
	if err := store.Update(ctx, []aggregation.SensorState{temperature(t0, 20), other}); err != nil {
		t.Fatal(err)
	}
	got, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].GreenhouseID != "gh-000" || got[1].GreenhouseID != "gh-001" {
		t.Fatalf("lista = %+v", got)
	}
	if got[1].Sensors["TEMPERATURE"].Mean != 20 {
		t.Fatalf("sensor = %+v", got[1].Sensors)
	}
}
