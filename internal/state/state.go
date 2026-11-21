// Package state guarda e expõe o último estado de cada estufa: o agregado
// mais recente de cada grandeza e o alerta que ele gerou. É uma projeção para
// leitura rápida; o tópico de agregados e o Postgres são a fonte da verdade.
package state

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound indica estufa sem estado conhecido.
var ErrNotFound = errors.New("estufa sem estado")

// Sensor é o último agregado de uma grandeza.
type Sensor struct {
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	SampleCount int       `json:"sample_count"`
	Mean        float64   `json:"mean"`
	Min         float64   `json:"min"`
	Max         float64   `json:"max"`
	Unit        string    `json:"unit"`
	Alert       *Alert    `json:"alert,omitempty"`
}

// Alert resume o alerta da janela.
type Alert struct {
	Rule     string  `json:"rule"`
	Observed float64 `json:"observed"`
	Limit    float64 `json:"limit"`
}

// Greenhouse é o estado de uma estufa, por tipo de sensor.
type Greenhouse struct {
	GreenhouseID string            `json:"greenhouse_id"`
	Sensors      map[string]Sensor `json:"sensors"`
}

// Reader lê o estado de uma estufa.
type Reader interface {
	Get(ctx context.Context, greenhouseID string) (Greenhouse, error)
}

// Lister lista as estufas com estado conhecido.
type Lister interface {
	List(ctx context.Context) ([]Greenhouse, error)
}
