package config_test

import (
	"testing"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/config"
	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

func TestEnvDefaultsAndOverrides(t *testing.T) {
	t.Setenv("KT_BROKERS", "a:9092, b:9092,")
	t.Setenv("KT_PARTITIONS", "12")
	t.Setenv("KT_LATENESS", "15s")
	t.Setenv("KT_RATIO", "0.25")
	var env config.Env
	if got := env.List("KT_BROKERS", "x"); len(got) != 2 || got[1] != "b:9092" {
		t.Errorf("List = %v", got)
	}
	if env.Int("KT_PARTITIONS", 1) != 12 || env.Duration("KT_LATENESS", 0) != 15*time.Second || env.Float("KT_RATIO", 0) != 0.25 {
		t.Error("override não aplicado")
	}
	if env.String("KT_MISSING", "padrão") != "padrão" || env.Int("KT_MISSING", 3) != 3 {
		t.Error("padrão não aplicado")
	}
	if err := env.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestEnvAccumulatesErrors(t *testing.T) {
	t.Setenv("KT_PARTITIONS", "doze")
	t.Setenv("KT_LATENESS", "15 segundos")
	t.Setenv("KT_RATIO", "um quarto")
	var env config.Env
	env.Int("KT_PARTITIONS", 1)
	env.Duration("KT_LATENESS", 0)
	env.Float("KT_RATIO", 0)
	if err := env.Err(); err == nil {
		t.Fatal("esperava erros acumulados")
	}
}

func TestThresholdsOverrideAndValidation(t *testing.T) {
	t.Setenv("THRESHOLD_TEMPERATURE_MAX", "28.5")
	var e config.Env
	th := e.Thresholds()
	if err := e.Err(); err != nil {
		t.Fatal(err)
	}
	if got := th[telemetry.SensorTemperature]; got.Min != 12 || got.Max != 28.5 {
		t.Fatalf("limite de temperatura = %+v", got)
	}
	if got := th[telemetry.SensorHumidity]; got.Min != 50 || got.Max != 90 {
		t.Fatalf("umidade deveria manter o padrão, veio %+v", got)
	}

	t.Setenv("THRESHOLD_HUMIDITY_MIN", "95")
	var bad config.Env
	bad.Thresholds()
	if bad.Err() == nil {
		t.Fatal("mínimo acima do máximo deveria ser erro")
	}
}
