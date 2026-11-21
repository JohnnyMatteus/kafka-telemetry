package contract

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
)

// ErrUnknownSchema indica que o id do schema não existe no registry. A
// mensagem nunca vai ser decodificável, então é tratada como inválida.
var ErrUnknownSchema = errors.New("unknown schema id")

// Registry é a porta para o Schema Registry. Implementações devem devolver um
// erro que satisfaça errors.Is(err, ErrUnknownSchema) quando o id não existir;
// qualquer outro erro é considerado transitório.
type Registry interface {
	SchemaByID(ctx context.Context, id int) (string, error)
}

// Encoder serializa valores com um schema já registrado.
type Encoder struct {
	id     uint32
	schema avro.Schema
}

// NewEncoder cria um encoder para o schema registrado com o id informado.
func NewEncoder(id int, schemaText string) (*Encoder, error) {
	if id < 0 {
		return nil, fmt.Errorf("schema id inválido: %d", id)
	}
	s, err := avro.Parse(schemaText)
	if err != nil {
		return nil, fmt.Errorf("parse schema: %w", err)
	}
	return &Encoder{id: uint32(id), schema: s}, nil
}

// Encode serializa v no formato de fio do Schema Registry.
func (e *Encoder) Encode(v any) ([]byte, error) {
	payload, err := avro.Marshal(e.schema, v)
	if err != nil {
		return nil, fmt.Errorf("avro marshal: %w", err)
	}
	return AppendFrame(make([]byte, 0, headerSize+len(payload)), e.id, payload), nil
}

// Decoder desserializa mensagens de qualquer versão de schema, buscando o
// schema do escritor no registry e guardando-o em cache. O registro Go de
// destino conhece todos os campos da versão mais nova; campos ausentes na
// versão do escritor ficam com o valor zero (por isso os campos novos são
// opcionais: é o que a compatibilidade BACKWARD garante).
type Decoder struct {
	registry Registry
	mu       sync.RWMutex
	cache    map[uint32]avro.Schema
}

// NewDecoder cria um decoder sobre o registry informado.
func NewDecoder(r Registry) *Decoder {
	return &Decoder{registry: r, cache: make(map[uint32]avro.Schema)}
}

// Decode desserializa b em v. Erros que envolvem ErrMalformed ou
// ErrUnknownSchema são definitivos; os demais vêm do registry e são transitórios.
func (d *Decoder) Decode(ctx context.Context, b []byte, v any) error {
	id, payload, err := ParseFrame(b)
	if err != nil {
		return err
	}
	schema, err := d.writerSchema(ctx, id)
	if err != nil {
		return err
	}
	if err := avro.Unmarshal(schema, payload, v); err != nil {
		return fmt.Errorf("%w: schema %d: %v", ErrMalformed, id, err)
	}
	return nil
}

func (d *Decoder) writerSchema(ctx context.Context, id uint32) (avro.Schema, error) {
	d.mu.RLock()
	s, ok := d.cache[id]
	d.mu.RUnlock()
	if ok {
		return s, nil
	}

	text, err := d.registry.SchemaByID(ctx, int(id))
	if err != nil {
		return nil, fmt.Errorf("schema %d: %w", id, err)
	}
	s, err = avro.Parse(text)
	if err != nil {
		return nil, fmt.Errorf("%w: schema %d não é Avro válido: %v", ErrMalformed, id, err)
	}
	d.mu.Lock()
	d.cache[id] = s
	d.mu.Unlock()
	return s, nil
}
