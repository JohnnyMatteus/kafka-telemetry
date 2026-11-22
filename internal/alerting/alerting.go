// Package alerting modela o alerta persistido e o seu reconhecimento por um
// operador. O alerta nasce no agregador; aqui só se consulta e se reconhece.
package alerting

import (
	"context"
	"errors"
	"time"
)

// Erros de domínio do reconhecimento.
var (
	ErrNotFound            = errors.New("alerta inexistente")
	ErrAlreadyAcknowledged = errors.New("alerta já reconhecido")
)

// Status filtra alertas pelo reconhecimento.
type Status string

const (
	StatusAny          Status = ""
	StatusOpen         Status = "open"
	StatusAcknowledged Status = "acknowledged"
)

// Valid informa se o status é aceito em consultas.
func (s Status) Valid() bool {
	return s == StatusAny || s == StatusOpen || s == StatusAcknowledged
}

// Alert é um alerta de limite com o estado de reconhecimento.
type Alert struct {
	ID             string
	GreenhouseID   string
	SensorType     string
	WindowStart    time.Time
	WindowEnd      time.Time
	Rule           string
	ObservedValue  float64
	LimitValue     float64
	AcknowledgedAt *time.Time
	// AcknowledgedBy é o operador que reconheceu, vindo do token e nunca do corpo.
	AcknowledgedBy *string
	Note           *string
}

// Repository é a porta de persistência dos alertas.
type Repository interface {
	List(ctx context.Context, status Status, limit int) ([]Alert, error)
	// Acknowledge devolve ErrNotFound ou ErrAlreadyAcknowledged.
	Acknowledge(ctx context.Context, id, operator, note string, at time.Time) (Alert, error)
}
