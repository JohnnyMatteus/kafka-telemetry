package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

// Registry adapta o cliente do Schema Registry às necessidades do pipeline.
type Registry struct {
	client *sr.Client
}

// NewRegistry cria o adaptador para a URL informada.
func NewRegistry(url string) (*Registry, error) {
	cl, err := sr.NewClient(sr.URLs(url))
	if err != nil {
		return nil, fmt.Errorf("schema registry: %w", err)
	}
	return &Registry{client: cl}, nil
}

// SchemaByID implementa contract.Registry.
func (r *Registry) SchemaByID(ctx context.Context, id int) (string, error) {
	s, err := r.client.SchemaByID(ctx, id)
	if err != nil {
		if isCode(err, sr.ErrSchemaNotFound.Code) {
			return "", fmt.Errorf("%w: %w", contract.ErrUnknownSchema, err)
		}
		return "", err
	}
	return s.Schema, nil
}

// SetBackward configura o subject com compatibilidade BACKWARD.
func (r *Registry) SetBackward(ctx context.Context, subject string) error {
	for _, res := range r.client.SetCompatibility(ctx, sr.SetCompatibility{Level: sr.CompatBackward}, subject) {
		if res.Err != nil {
			return fmt.Errorf("compatibilidade de %s: %w", subject, res.Err)
		}
	}
	return nil
}

// CheckCompatible pergunta ao registry se o schema pode virar a próxima
// versão do subject. Subject inexistente é compatível por definição.
func (r *Registry) CheckCompatible(ctx context.Context, subject, schema string) (bool, []string, error) {
	res, err := r.client.CheckCompatibility(ctx, subject, -1, sr.Schema{Schema: schema, Type: sr.TypeAvro})
	if err != nil {
		if isCode(err, sr.ErrSubjectNotFound.Code) || isCode(err, sr.ErrVersionNotFound.Code) {
			return true, nil, nil
		}
		return false, nil, fmt.Errorf("checar compatibilidade de %s: %w", subject, err)
	}
	return res.Is, res.Messages, nil
}

// Register registra o schema no subject e devolve o id. Registrar de novo um
// schema idêntico devolve o mesmo id, então a operação é idempotente.
func (r *Registry) Register(ctx context.Context, subject, schema string) (int, error) {
	ss, err := r.client.CreateSchema(ctx, subject, sr.Schema{Schema: schema, Type: sr.TypeAvro})
	if err != nil {
		return 0, fmt.Errorf("registrar %s: %w", subject, err)
	}
	return ss.ID, nil
}

// Lookup devolve o id de um schema já registrado no subject, sem registrar.
func (r *Registry) Lookup(ctx context.Context, subject, schema string) (int, error) {
	ss, err := r.client.LookupSchema(ctx, subject, sr.Schema{Schema: schema, Type: sr.TypeAvro})
	if err != nil {
		return 0, fmt.Errorf("schema de %s não registrado (rode telemetryctl schemas register): %w", subject, err)
	}
	return ss.ID, nil
}

func isCode(err error, code int) bool {
	var re *sr.ResponseError
	return errors.As(err, &re) && re.ErrorCode == code
}

// EncoderFor registra (ou reencontra) a versão mais nova do schema do tópico e
// devolve o encoder dela.
func (r *Registry) EncoderFor(ctx context.Context, topic string) (*contract.Encoder, error) {
	sub, err := schemas.Lookup(topic)
	if err != nil {
		return nil, err
	}
	text, err := sub.Latest()
	if err != nil {
		return nil, err
	}
	id, err := r.Lookup(ctx, sub.Name, text)
	if err != nil {
		return nil, err
	}
	return contract.NewEncoder(id, text)
}
