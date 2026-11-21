package telemetry

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// Limit é a faixa agronômica aceitável de uma grandeza. Diferente da faixa
// física de Reading.Validate: sair dela é um evento de negócio, não um erro.
type Limit struct {
	Min float64
	Max float64
}

// Thresholds associa cada tipo de sensor ao seu limite.
type Thresholds map[SensorType]Limit

// DefaultThresholds são limites típicos para cultivo de hortaliças em estufa.
func DefaultThresholds() Thresholds {
	return Thresholds{
		SensorTemperature: {Min: 12, Max: 32},
		SensorHumidity:    {Min: 50, Max: 90},
		SensorCO2:         {Min: 350, Max: 1500},
		SensorLuminosity:  {Min: 0, Max: 80000},
	}
}

// AlertRule identifica qual lado do limite foi ultrapassado.
type AlertRule string

const (
	RuleAboveMax AlertRule = "ABOVE_MAX"
	RuleBelowMin AlertRule = "BELOW_MIN"
)

// Alert é emitido quando a média de uma janela sai do limite.
type Alert struct {
	AlertID      string
	GreenhouseID string
	Type         SensorType
	WindowStart  time.Time
	WindowEnd    time.Time
	Rule         AlertRule
	Observed     float64
	Limit        float64
}

// Evaluate compara a média da janela com o limite do tipo de sensor.
// A média, e não o máximo, evita alertas por um pico isolado de um sensor.
// O id do alerta é derivado da janela e da regra, então reemitir a mesma
// janela gera o mesmo id e o consumidor do alerta pode deduplicar.
func (t Thresholds) Evaluate(a Aggregate) (Alert, bool) {
	limit, ok := t[a.Key.Type]
	if !ok {
		return Alert{}, false
	}
	var rule AlertRule
	var bound float64
	switch {
	case a.Mean > limit.Max:
		rule, bound = RuleAboveMax, limit.Max
	case a.Mean < limit.Min:
		rule, bound = RuleBelowMin, limit.Min
	default:
		return Alert{}, false
	}
	return Alert{
		AlertID:      alertID(a.Key, rule),
		GreenhouseID: a.Key.GreenhouseID,
		Type:         a.Key.Type,
		WindowStart:  a.Key.Start,
		WindowEnd:    a.End,
		Rule:         rule,
		Observed:     a.Mean,
		Limit:        bound,
	}, true
}

func alertID(k WindowKey, rule AlertRule) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s|%s|%d|%s", k.GreenhouseID, k.Type, k.Start.UnixMilli(), rule))
	return hex.EncodeToString(sum[:16])
}
