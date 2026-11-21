package telemetry_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

var base = time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

func validReading() telemetry.Reading {
	return telemetry.Reading{
		EventID:      "evt-1",
		GreenhouseID: "gh-001",
		SensorID:     "gh-001-temperature-1",
		Type:         telemetry.SensorTemperature,
		Value:        24.5,
		MeasuredAt:   base,
	}
}

func TestReadingValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*telemetry.Reading)
		wantErr bool
	}{
		{name: "leitura válida", mutate: func(*telemetry.Reading) {}},
		{name: "event_id vazio", mutate: func(r *telemetry.Reading) { r.EventID = " " }, wantErr: true},
		{name: "estufa vazia", mutate: func(r *telemetry.Reading) { r.GreenhouseID = "" }, wantErr: true},
		{name: "sensor vazio", mutate: func(r *telemetry.Reading) { r.SensorID = "" }, wantErr: true},
		{name: "tipo desconhecido", mutate: func(r *telemetry.Reading) { r.Type = "PH" }, wantErr: true},
		{name: "NaN", mutate: func(r *telemetry.Reading) { r.Value = math.NaN() }, wantErr: true},
		{name: "infinito", mutate: func(r *telemetry.Reading) { r.Value = math.Inf(1) }, wantErr: true},
		{name: "umidade acima de 100", mutate: func(r *telemetry.Reading) {
			r.Type, r.Value = telemetry.SensorHumidity, 100.1
		}, wantErr: true},
		{name: "umidade no limite físico", mutate: func(r *telemetry.Reading) {
			r.Type, r.Value = telemetry.SensorHumidity, 100
		}},
		{name: "CO2 negativo", mutate: func(r *telemetry.Reading) {
			r.Type, r.Value = telemetry.SensorCO2, -1
		}, wantErr: true},
		{name: "sem horário", mutate: func(r *telemetry.Reading) { r.MeasuredAt = time.Time{} }, wantErr: true},
		{name: "relógio adiantado além da tolerância", mutate: func(r *telemetry.Reading) {
			r.MeasuredAt = base.Add(6 * time.Minute)
		}, wantErr: true},
		{name: "relógio adiantado dentro da tolerância", mutate: func(r *telemetry.Reading) {
			r.MeasuredAt = base.Add(4 * time.Minute)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validReading()
			tt.mutate(&r)
			err := r.Validate(base)
			if tt.wantErr {
				if !errors.Is(err, telemetry.ErrInvalidReading) {
					t.Fatalf("esperava ErrInvalidReading, veio %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("erro inesperado: %v", err)
			}
		})
	}
}

func TestReadingValidateAccumulatesProblems(t *testing.T) {
	r := telemetry.Reading{Type: telemetry.SensorCO2, Value: math.NaN(), MeasuredAt: base}
	err := r.Validate(base)
	want := "invalid reading: event_id vazio; greenhouse_id vazio; sensor_id vazio; valor não numérico"
	if err == nil || err.Error() != want {
		t.Fatalf("mensagem = %v, quero %q", err, want)
	}
}

func TestSensorTypeUnit(t *testing.T) {
	for _, st := range telemetry.AllSensorTypes {
		if !st.Valid() || st.Unit() == "" {
			t.Errorf("%s sem unidade ou inválido", st)
		}
	}
	if telemetry.SensorType("PH").Valid() {
		t.Error("PH não deveria ser válido")
	}
}
