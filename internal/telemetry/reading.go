// Package telemetry contém o modelo de domínio da telemetria das estufas:
// leituras de sensores, janelas de agregação, limites e alertas.
// O pacote é puro: não conhece Kafka, Schema Registry nem relógio de parede.
package telemetry

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// SensorType identifica a grandeza medida por um sensor.
type SensorType string

const (
	SensorTemperature SensorType = "TEMPERATURE"
	SensorHumidity    SensorType = "HUMIDITY"
	SensorCO2         SensorType = "CO2"
	SensorLuminosity  SensorType = "LUMINOSITY"
)

// AllSensorTypes lista os tipos suportados, em ordem estável.
var AllSensorTypes = []SensorType{SensorTemperature, SensorHumidity, SensorCO2, SensorLuminosity}

// physicalRange é a faixa fisicamente plausível de cada grandeza. Valores fora
// dela indicam sensor defeituoso ou mensagem corrompida, não um alerta agronômico.
type physicalRange struct {
	min, max float64
	unit     string
}

var physicalRanges = map[SensorType]physicalRange{
	SensorTemperature: {min: -40, max: 85, unit: "celsius"},
	SensorHumidity:    {min: 0, max: 100, unit: "percent"},
	SensorCO2:         {min: 0, max: 10000, unit: "ppm"},
	SensorLuminosity:  {min: 0, max: 200000, unit: "lux"},
}

// Unit devolve a unidade canônica do tipo de sensor.
func (s SensorType) Unit() string { return physicalRanges[s].unit }

// Valid informa se o tipo é conhecido.
func (s SensorType) Valid() bool {
	_, ok := physicalRanges[s]
	return ok
}

// ErrInvalidReading indica uma leitura que viola o contrato do domínio.
// Leituras inválidas não são reprocessáveis: seguem para a DLQ.
var ErrInvalidReading = errors.New("invalid reading")

// maxClockSkew é o quanto uma leitura pode estar no futuro em relação ao
// horário de recebimento antes de ser considerada inválida.
const maxClockSkew = 5 * time.Minute

// Reading é uma medição única de um sensor de uma estufa.
type Reading struct {
	EventID      string
	GreenhouseID string
	SensorID     string
	Type         SensorType
	Value        float64
	MeasuredAt   time.Time
}

// Validate confere as invariantes da leitura. receivedAt é o instante em que
// a mensagem foi recebida, usado para rejeitar relógios de sensor adiantados.
func (r Reading) Validate(receivedAt time.Time) error {
	var problems []string
	if strings.TrimSpace(r.EventID) == "" {
		problems = append(problems, "event_id vazio")
	}
	if strings.TrimSpace(r.GreenhouseID) == "" {
		problems = append(problems, "greenhouse_id vazio")
	}
	if strings.TrimSpace(r.SensorID) == "" {
		problems = append(problems, "sensor_id vazio")
	}
	if r.MeasuredAt.IsZero() {
		problems = append(problems, "measured_at ausente")
	} else if r.MeasuredAt.After(receivedAt.Add(maxClockSkew)) {
		problems = append(problems, fmt.Sprintf("measured_at %s no futuro", r.MeasuredAt.UTC().Format(time.RFC3339)))
	}
	rng, known := physicalRanges[r.Type]
	switch {
	case !known:
		problems = append(problems, fmt.Sprintf("sensor_type desconhecido %q", r.Type))
	case math.IsNaN(r.Value) || math.IsInf(r.Value, 0):
		problems = append(problems, "valor não numérico")
	case r.Value < rng.min || r.Value > rng.max:
		problems = append(problems, fmt.Sprintf("valor %.2f fora da faixa física [%.0f, %.0f] %s", r.Value, rng.min, rng.max, rng.unit))
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrInvalidReading, strings.Join(problems, "; "))
	}
	return nil
}
