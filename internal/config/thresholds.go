package config

import (
	"fmt"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

// Thresholds parte dos limites padrão e sobrescreve cada um por
// THRESHOLD_<TIPO>_MIN e THRESHOLD_<TIPO>_MAX (ex.: THRESHOLD_TEMPERATURE_MAX=30).
// Limite com mínimo >= máximo é erro de configuração.
func (e *Env) Thresholds() telemetry.Thresholds {
	out := telemetry.DefaultThresholds()
	for _, t := range telemetry.AllSensorTypes {
		l := out[t]
		l.Min = e.Float(fmt.Sprintf("THRESHOLD_%s_MIN", t), l.Min)
		l.Max = e.Float(fmt.Sprintf("THRESHOLD_%s_MAX", t), l.Max)
		if l.Min >= l.Max {
			e.errs = append(e.errs, fmt.Errorf("THRESHOLD_%s: mínimo %v deve ser menor que o máximo %v", t, l.Min, l.Max))
		}
		out[t] = l
	}
	return out
}
