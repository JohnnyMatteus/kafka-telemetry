package telemetry_test

import (
	"testing"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

func TestDeduplicator(t *testing.T) {
	d := telemetry.NewDeduplicator()
	if d.Seen("a", base) {
		t.Fatal("primeira vez não é duplicata")
	}
	if !d.Seen("a", base.Add(time.Hour)) {
		t.Fatal("mesmo id deveria ser duplicata, mesmo com horário diferente")
	}
	d.Seen("b", base.Add(time.Minute))

	d.EvictBefore(base.Add(30 * time.Second))
	if d.Len() != 1 {
		t.Fatalf("retidos = %d, quero 1", d.Len())
	}
	if d.Seen("a", base) {
		t.Fatal("id esquecido volta a ser aceito")
	}
	if !d.Seen("b", base.Add(time.Minute)) {
		t.Fatal("id dentro da retenção continua duplicado")
	}
}

func aggregate(st telemetry.SensorType, mean float64) telemetry.Aggregate {
	return telemetry.Aggregate{
		Key:   telemetry.WindowKey{GreenhouseID: "gh-007", Type: st, Start: base},
		End:   base.Add(time.Minute),
		Count: 4, Min: mean - 1, Max: mean + 1, Mean: mean,
	}
}

func TestThresholdsEvaluate(t *testing.T) {
	th := telemetry.DefaultThresholds()
	tests := []struct {
		name     string
		agg      telemetry.Aggregate
		wantRule telemetry.AlertRule
		alert    bool
	}{
		{name: "temperatura normal", agg: aggregate(telemetry.SensorTemperature, 25)},
		{name: "temperatura exatamente no limite", agg: aggregate(telemetry.SensorTemperature, 32)},
		{name: "calor excessivo", agg: aggregate(telemetry.SensorTemperature, 33.5), wantRule: telemetry.RuleAboveMax, alert: true},
		{name: "umidade baixa", agg: aggregate(telemetry.SensorHumidity, 41), wantRule: telemetry.RuleBelowMin, alert: true},
		{name: "CO2 alto", agg: aggregate(telemetry.SensorCO2, 1800), wantRule: telemetry.RuleAboveMax, alert: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alert, ok := th.Evaluate(tt.agg)
			if ok != tt.alert {
				t.Fatalf("alerta = %v, quero %v", ok, tt.alert)
			}
			if ok && (alert.Rule != tt.wantRule || alert.Observed != tt.agg.Mean || alert.GreenhouseID != "gh-007") {
				t.Fatalf("alerta inesperado: %+v", alert)
			}
		})
	}
}

func TestAlertIDIsDeterministic(t *testing.T) {
	th := telemetry.DefaultThresholds()
	a1, _ := th.Evaluate(aggregate(telemetry.SensorTemperature, 35))
	a2, _ := th.Evaluate(aggregate(telemetry.SensorTemperature, 36))
	if a1.AlertID != a2.AlertID {
		t.Fatal("a mesma janela e regra devem gerar o mesmo id, mesmo que o valor mude no replay")
	}
	other := aggregate(telemetry.SensorTemperature, 35)
	other.Key.Start = base.Add(time.Minute)
	a3, _ := th.Evaluate(other)
	if a3.AlertID == a1.AlertID {
		t.Fatal("janelas diferentes precisam de ids diferentes")
	}
}

func TestThresholdsIgnoreUnknownType(t *testing.T) {
	th := telemetry.Thresholds{}
	if _, ok := th.Evaluate(aggregate(telemetry.SensorTemperature, 99)); ok {
		t.Fatal("sem limite configurado não há alerta")
	}
}
