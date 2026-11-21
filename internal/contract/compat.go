package contract

import (
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"
)

// Compatibility é o nível de compatibilidade, com a mesma semântica do
// Schema Registry.
type Compatibility string

const (
	// Backward: a versão nova lê dados escritos pela anterior. Permite
	// atualizar consumidores antes dos produtores.
	Backward Compatibility = "BACKWARD"
	// BackwardTransitive: a versão nova lê dados de todas as anteriores.
	BackwardTransitive Compatibility = "BACKWARD_TRANSITIVE"
	// Forward: a versão anterior lê dados escritos pela nova.
	Forward Compatibility = "FORWARD"
	// Full: Backward e Forward ao mesmo tempo.
	Full Compatibility = "FULL"
)

// ErrIncompatible indica que a mudança de schema quebra o nível exigido.
var ErrIncompatible = errors.New("incompatible schema change")

// CheckCompatibility confere se candidate pode ser registrado depois de
// previous (da versão mais antiga à mais nova) no nível informado.
func CheckCompatibility(level Compatibility, candidate string, previous ...string) error {
	if len(previous) == 0 {
		_, err := avro.Parse(candidate)
		return err
	}
	next, err := avro.Parse(candidate)
	if err != nil {
		return fmt.Errorf("parse candidato: %w", err)
	}
	olds := make([]avro.Schema, len(previous))
	for i, text := range previous {
		if olds[i], err = avro.Parse(text); err != nil {
			return fmt.Errorf("parse versão %d: %w", i+1, err)
		}
	}

	checker := avro.NewSchemaCompatibility()
	latest := olds[len(olds)-1]
	switch level {
	case Backward:
		return wrapIncompatible(checker.Compatible(next, latest), "BACKWARD", len(olds))
	case BackwardTransitive:
		for i, old := range olds {
			if err := wrapIncompatible(checker.Compatible(next, old), "BACKWARD_TRANSITIVE", i+1); err != nil {
				return err
			}
		}
		return nil
	case Forward:
		return wrapIncompatible(checker.Compatible(latest, next), "FORWARD", len(olds))
	case Full:
		if err := wrapIncompatible(checker.Compatible(next, latest), "FULL (backward)", len(olds)); err != nil {
			return err
		}
		return wrapIncompatible(checker.Compatible(latest, next), "FULL (forward)", len(olds))
	default:
		return fmt.Errorf("nível de compatibilidade não suportado: %q", level)
	}
}

func wrapIncompatible(err error, level string, version int) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %s em relação à versão %d: %v", ErrIncompatible, level, version, err)
}
