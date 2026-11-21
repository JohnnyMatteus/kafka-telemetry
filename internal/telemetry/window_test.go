package telemetry_test

import (
	"errors"
	"testing"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

var cfg = telemetry.WindowConfig{Size: time.Minute, AllowedLateness: 10 * time.Second}

func newWindows(t *testing.T) *telemetry.PartitionWindows {
	t.Helper()
	p, err := telemetry.NewPartitionWindows(cfg, time.Time{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func reading(id, gh string, st telemetry.SensorType, v float64, at time.Time) telemetry.Reading {
	return telemetry.Reading{EventID: id, GreenhouseID: gh, SensorID: gh + "-s", Type: st, Value: v, MeasuredAt: at}
}

func TestWindowAggregatesMeanMinMax(t *testing.T) {
	p := newWindows(t)
	values := []float64{20, 26, 23}
	for i, v := range values {
		at := base.Add(time.Duration(i*15) * time.Second)
		if got := p.Add(reading(string(rune('a'+i)), "gh-001", telemetry.SensorTemperature, v, at), int64(i)); got != telemetry.OutcomeAccepted {
			t.Fatalf("leitura %d: outcome %s", i, got)
		}
	}
	if closed := p.CloseReady(); len(closed) != 0 {
		t.Fatalf("janela fechou cedo demais: %+v", closed)
	}

	// Leitura no minuto seguinte, além da tolerância: fecha a primeira janela.
	p.Add(reading("d", "gh-001", telemetry.SensorTemperature, 30, base.Add(70*time.Second)), 3)
	closed := p.CloseReady()
	if len(closed) != 1 {
		t.Fatalf("esperava 1 janela fechada, veio %d", len(closed))
	}
	got := closed[0]
	want := telemetry.Aggregate{
		Key:   telemetry.WindowKey{GreenhouseID: "gh-001", Type: telemetry.SensorTemperature, Start: base},
		End:   base.Add(time.Minute),
		Count: 3, Min: 20, Max: 26, Mean: 23,
	}
	if got != want {
		t.Fatalf("agregado = %+v, quero %+v", got, want)
	}
}

func TestWindowsAreIsolatedByGreenhouseAndSensor(t *testing.T) {
	p := newWindows(t)
	p.Add(reading("1", "gh-001", telemetry.SensorTemperature, 20, base), 0)
	p.Add(reading("2", "gh-002", telemetry.SensorTemperature, 30, base), 1)
	p.Add(reading("3", "gh-001", telemetry.SensorHumidity, 70, base), 2)
	p.Add(reading("4", "gh-001", telemetry.SensorTemperature, 22, base.Add(2*time.Minute)), 3)

	closed := p.CloseReady()
	if len(closed) != 3 {
		t.Fatalf("esperava 3 janelas, veio %d", len(closed))
	}
	order := []struct {
		gh string
		st telemetry.SensorType
	}{{"gh-001", telemetry.SensorHumidity}, {"gh-001", telemetry.SensorTemperature}, {"gh-002", telemetry.SensorTemperature}}
	for i, o := range order {
		if closed[i].Key.GreenhouseID != o.gh || closed[i].Key.Type != o.st || closed[i].Count != 1 {
			t.Errorf("posição %d = %+v, quero %s/%s", i, closed[i].Key, o.gh, o.st)
		}
	}
}

func TestLateReadingWithinToleranceIsAccepted(t *testing.T) {
	p := newWindows(t)
	p.Add(reading("1", "gh-001", telemetry.SensorCO2, 400, base.Add(65*time.Second)), 0)
	// 5s depois do fim da janela anterior, dentro dos 10s de tolerância.
	if got := p.Add(reading("2", "gh-001", telemetry.SensorCO2, 420, base.Add(59*time.Second)), 1); got != telemetry.OutcomeAccepted {
		t.Fatalf("outcome %s, esperava accepted", got)
	}
}

func TestLateReadingBeyondToleranceIsDropped(t *testing.T) {
	p := newWindows(t)
	p.Add(reading("1", "gh-001", telemetry.SensorCO2, 400, base.Add(75*time.Second)), 0)
	if got := p.Add(reading("2", "gh-001", telemetry.SensorCO2, 420, base.Add(30*time.Second)), 1); got != telemetry.OutcomeLate {
		t.Fatalf("outcome %s, esperava late", got)
	}
	if p.CommitOffset() != 0 {
		t.Fatalf("commit = %d, a janela aberta começa no offset 0", p.CommitOffset())
	}
}

func TestDuplicateEventIsCountedOnce(t *testing.T) {
	p := newWindows(t)
	r := reading("evt-42", "gh-001", telemetry.SensorHumidity, 60, base)
	if got := p.Add(r, 0); got != telemetry.OutcomeAccepted {
		t.Fatalf("primeira entrega: %s", got)
	}
	r.Value = 99 // retransmissão com o mesmo id não pode alterar o agregado
	if got := p.Add(r, 1); got != telemetry.OutcomeDuplicate {
		t.Fatalf("segunda entrega: %s", got)
	}
	p.Add(reading("evt-43", "gh-001", telemetry.SensorHumidity, 70, base.Add(2*time.Minute)), 2)
	closed := p.CloseReady()
	if len(closed) != 1 || closed[0].Count != 1 || closed[0].Max != 60 {
		t.Fatalf("agregado contou a duplicata: %+v", closed)
	}
}

func TestCommitOffsetNeverPassesOpenWindow(t *testing.T) {
	p := newWindows(t)
	if p.CommitOffset() != 0 {
		t.Fatalf("partição vazia deveria commitar o offset inicial")
	}
	p.Add(reading("1", "gh-001", telemetry.SensorTemperature, 20, base), 10)
	p.Add(reading("2", "gh-002", telemetry.SensorTemperature, 21, base.Add(30*time.Second)), 11)
	p.Skip(12) // mensagem inválida já enviada para a DLQ
	if got := p.CommitOffset(); got != 10 {
		t.Fatalf("commit = %d, quero 10 (primeiro registro de janela aberta)", got)
	}

	p.Add(reading("3", "gh-001", telemetry.SensorTemperature, 22, base.Add(80*time.Second)), 13)
	p.CloseReady()
	if got := p.CommitOffset(); got != 13 {
		t.Fatalf("commit = %d, quero 13 depois de fechar as janelas do primeiro minuto", got)
	}
	if p.OpenWindows() != 1 {
		t.Fatalf("janelas abertas = %d", p.OpenWindows())
	}
}

func TestCommitOffsetAdvancesWhenNothingIsOpen(t *testing.T) {
	p := newWindows(t)
	p.Skip(0)
	p.Skip(1)
	if got := p.CommitOffset(); got != 2 {
		t.Fatalf("commit = %d, quero 2", got)
	}
}

// Depois de um restart, o consumo recomeça no offset commitado com o
// watermark do checkpoint. Leituras de janelas que já foram emitidas são
// descartadas como atrasadas e as janelas abertas são reconstruídas inteiras.
func TestReplayFromCheckpointIsDeterministic(t *testing.T) {
	events := []struct {
		id string
		gh string
		v  float64
		at time.Duration
	}{
		{"a", "gh-001", 20, 0},
		{"b", "gh-002", 30, 50 * time.Second},
		{"c", "gh-001", 22, 55 * time.Second},
		{"d", "gh-002", 31, 65 * time.Second},
		{"e", "gh-001", 24, 72 * time.Second}, // fecha o primeiro minuto
		{"f", "gh-002", 32, 58 * time.Second}, // atrasada além da tolerância
		{"g", "gh-001", 25, 90 * time.Second},
	}

	original := newWindows(t)
	var commitAt int64
	var checkpoint time.Time
	var emitted []telemetry.Aggregate
	for i, e := range events {
		original.Add(reading(e.id, e.gh, telemetry.SensorTemperature, e.v, base.Add(e.at)), int64(i))
		emitted = append(emitted, original.CloseReady()...)
		if i == 4 {
			commitAt, checkpoint = original.CommitOffset(), original.Watermark()
		}
	}
	original.Add(reading("h", "gh-001", telemetry.SensorTemperature, 26, base.Add(3*time.Minute)), 7)
	emitted = append(emitted, original.CloseReady()...)

	replayed, err := telemetry.NewPartitionWindows(cfg, checkpoint, commitAt)
	if err != nil {
		t.Fatal(err)
	}
	var reemitted []telemetry.Aggregate
	for i := commitAt; i < int64(len(events)); i++ {
		e := events[i]
		replayed.Add(reading(e.id, e.gh, telemetry.SensorTemperature, e.v, base.Add(e.at)), i)
		reemitted = append(reemitted, replayed.CloseReady()...)
	}
	replayed.Add(reading("h", "gh-001", telemetry.SensorTemperature, 26, base.Add(3*time.Minute)), 7)
	reemitted = append(reemitted, replayed.CloseReady()...)

	// Tudo que o replay emite precisa ser idêntico ao que já tinha sido emitido.
	byKey := map[telemetry.WindowKey]telemetry.Aggregate{}
	for _, a := range emitted {
		byKey[a.Key] = a
	}
	for _, a := range reemitted {
		if prev, ok := byKey[a.Key]; !ok || prev != a {
			t.Errorf("replay emitiu %+v, original %+v", a, prev)
		}
	}
	if len(reemitted) == 0 {
		t.Fatal("o replay deveria reemitir as janelas que estavam abertas no checkpoint")
	}
}

func TestInvalidWindowConfig(t *testing.T) {
	_, err := telemetry.NewPartitionWindows(telemetry.WindowConfig{Size: 0}, time.Time{}, 0)
	if !errors.Is(err, telemetry.ErrInvalidWindowConfig) {
		t.Fatalf("esperava ErrInvalidWindowConfig, veio %v", err)
	}
}
