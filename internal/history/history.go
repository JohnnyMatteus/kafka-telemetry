// Package history modela a série temporal de agregados de uma estufa.
package history

import (
	"context"
	"time"
)

// Point é um agregado de janela.
type Point struct {
	WindowStart time.Time
	WindowEnd   time.Time
	SampleCount int
	Mean        float64
	Min         float64
	Max         float64
}

// Series é a série de uma grandeza.
type Series struct {
	Unit   string
	Points []Point
}

// Repository é a porta de leitura das séries, por tipo de sensor.
type Repository interface {
	Series(ctx context.Context, greenhouseID string, from, to time.Time) (map[string]Series, error)
}
