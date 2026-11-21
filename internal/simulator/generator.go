// Package simulator gera leituras plausíveis dos sensores das estufas, com
// taxas configuráveis de retransmissão (duplicatas), leituras inválidas e
// estufas com anomalia (que disparam alertas).
package simulator

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/telemetry"
)

// Config controla o perfil da carga gerada.
type Config struct {
	Greenhouses    int
	DuplicateRatio float64 // fração de leituras retransmitidas com o mesmo event_id
	InvalidRatio   float64 // fração de leituras fora da faixa física (vão para a DLQ)
	AnomalyRatio   float64 // fração de estufas com temperatura acima do limite
	Seed           uint64
}

// ErrInvalidConfig indica parâmetros de simulação inválidos.
var ErrInvalidConfig = errors.New("invalid simulator config")

// Validate confere a configuração.
func (c Config) Validate() error {
	if c.Greenhouses <= 0 {
		return fmt.Errorf("%w: greenhouses deve ser positivo", ErrInvalidConfig)
	}
	for name, r := range map[string]float64{"duplicate": c.DuplicateRatio, "invalid": c.InvalidRatio, "anomaly": c.AnomalyRatio} {
		if r < 0 || r > 1 {
			return fmt.Errorf("%w: %s ratio %.2f fora de [0, 1]", ErrInvalidConfig, name, r)
		}
	}
	return nil
}

type sensor struct {
	greenhouseID string
	sensorID     string
	typ          telemetry.SensorType
	anomalous    bool
}

// baseline é o valor médio e a amplitude de variação de cada grandeza.
var baseline = map[telemetry.SensorType]struct{ mean, swing, noise float64 }{
	telemetry.SensorTemperature: {mean: 24, swing: 4, noise: 0.5},
	telemetry.SensorHumidity:    {mean: 70, swing: 8, noise: 1.5},
	telemetry.SensorCO2:         {mean: 800, swing: 150, noise: 20},
	telemetry.SensorLuminosity:  {mean: 30000, swing: 15000, noise: 800},
}

// Generator produz leituras percorrendo os sensores em rodízio.
type Generator struct {
	cfg     Config
	rng     *rand.Rand
	sensors []sensor
	cursor  int
	last    *telemetry.Reading
}

// New cria um gerador determinístico para a semente informada.
func New(cfg Config) (*Generator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9E3779B97F4A7C15))
	anomalous := int(math.Round(float64(cfg.Greenhouses) * cfg.AnomalyRatio))
	sensors := make([]sensor, 0, cfg.Greenhouses*len(telemetry.AllSensorTypes))
	for i := range cfg.Greenhouses {
		gh := GreenhouseID(i)
		for _, st := range telemetry.AllSensorTypes {
			sensors = append(sensors, sensor{
				greenhouseID: gh,
				sensorID:     fmt.Sprintf("%s-%s-1", gh, st),
				typ:          st,
				anomalous:    i < anomalous,
			})
		}
	}
	return &Generator{cfg: cfg, rng: rng, sensors: sensors}, nil
}

// GreenhouseID devolve o identificador da estufa de índice i.
func GreenhouseID(i int) string { return fmt.Sprintf("gh-%03d", i+1) }

// Sensors devolve quantos sensores o gerador simula.
func (g *Generator) Sensors() int { return len(g.sensors) }

// Next devolve a próxima leitura medida em now.
func (g *Generator) Next(now time.Time) telemetry.Reading {
	if g.last != nil && g.rng.Float64() < g.cfg.DuplicateRatio {
		dup := *g.last
		g.last = nil // no máximo uma retransmissão seguida
		return dup
	}

	s := g.sensors[g.cursor]
	g.cursor = (g.cursor + 1) % len(g.sensors)

	r := telemetry.Reading{
		EventID:      g.uuid(),
		GreenhouseID: s.greenhouseID,
		SensorID:     s.sensorID,
		Type:         s.typ,
		Value:        g.value(s, now),
		MeasuredAt:   now.UTC().Truncate(time.Millisecond),
	}
	if g.rng.Float64() < g.cfg.InvalidRatio {
		r.Value = -999 // fora de qualquer faixa física
	}
	g.last = &r
	return r
}

func (g *Generator) value(s sensor, now time.Time) float64 {
	b := baseline[s.typ]
	// Ciclo diário: pico às 14h, vale às 2h.
	hour := float64(now.UTC().Hour()) + float64(now.UTC().Minute())/60
	daily := math.Sin((hour - 8) / 24 * 2 * math.Pi)
	v := b.mean + b.swing*daily + g.rng.NormFloat64()*b.noise
	if s.anomalous && s.typ == telemetry.SensorTemperature {
		v += 14
	}
	if s.typ == telemetry.SensorHumidity {
		v = math.Min(math.Max(v, 0), 100)
	}
	return math.Max(math.Round(v*100)/100, 0)
}

// uuid gera um UUID v4 a partir do gerador da simulação, para que a mesma
// semente produza os mesmos ids.
func (g *Generator) uuid() string {
	var b [16]byte
	hi, lo := g.rng.Uint64(), g.rng.Uint64()
	for i := range 8 {
		b[i] = byte(hi >> (56 - 8*i))
		b[8+i] = byte(lo >> (56 - 8*i))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
