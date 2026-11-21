package contract_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/contract"
	"github.com/JohnnyMatteus/kafka-telemetry/schemas"
)

// TestRepositorySchemasAreBackwardCompatible é o portão de contrato do CI:
// toda versão nova de um subject precisa ler os dados de todas as anteriores.
// O registry está configurado como BACKWARD; aqui exigimos a forma transitiva,
// que é mais forte, para que um replay longo do tópico nunca quebre.
func TestRepositorySchemasAreBackwardCompatible(t *testing.T) {
	for _, sub := range schemas.Subjects {
		t.Run(sub.Name, func(t *testing.T) {
			texts, err := sub.Texts()
			if err != nil {
				t.Fatal(err)
			}
			for i := range texts {
				if err := contract.CheckCompatibility(contract.BackwardTransitive, texts[i], texts[:i]...); err != nil {
					t.Fatalf("%s: %v", sub.Versions[i], err)
				}
			}
		})
	}
}

func fixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "schemas", "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestSchemaChangeCompatibility(t *testing.T) {
	v1, v2 := readingVersions(t)
	tests := []struct {
		name      string
		candidate string
		level     contract.Compatibility
		wantOK    bool
	}{
		{name: "adicionar campo opcional passa em BACKWARD", candidate: v2, level: contract.Backward, wantOK: true},
		{name: "adicionar campo opcional passa em FULL", candidate: v2, level: contract.Full, wantOK: true},
		{name: "adicionar campo obrigatório sem default falha em BACKWARD", candidate: fixture(t, "sensor_reading.add-required-field.avsc"), level: contract.Backward},
		{name: "mudar o tipo de um campo falha em BACKWARD", candidate: fixture(t, "sensor_reading.change-value-type.avsc"), level: contract.Backward},
		// Remover um campo obrigatório não quebra BACKWARD (o leitor novo ignora
		// o campo antigo), mas quebra consumidores antigos: é FORWARD que falha.
		{name: "remover campo obrigatório passa em BACKWARD", candidate: fixture(t, "sensor_reading.remove-required-field.avsc"), level: contract.Backward, wantOK: true},
		{name: "remover campo obrigatório falha em FORWARD", candidate: fixture(t, "sensor_reading.remove-required-field.avsc"), level: contract.Forward},
		{name: "remover campo obrigatório falha em FULL", candidate: fixture(t, "sensor_reading.remove-required-field.avsc"), level: contract.Full},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := contract.CheckCompatibility(tt.level, tt.candidate, v1)
			if tt.wantOK && err != nil {
				t.Fatalf("esperava compatível, veio %v", err)
			}
			if !tt.wantOK && !errors.Is(err, contract.ErrIncompatible) {
				t.Fatalf("esperava ErrIncompatible, veio %v", err)
			}
		})
	}
}

func TestBackwardTransitiveChecksEveryVersion(t *testing.T) {
	v1, v2 := readingVersions(t)
	// Na forma transitiva, o candidato é comparado com v1 e com v2; o campo
	// obrigatório novo quebra já na primeira comparação.
	candidate := fixture(t, "sensor_reading.add-required-field.avsc")
	if err := contract.CheckCompatibility(contract.BackwardTransitive, candidate, v1, v2); !errors.Is(err, contract.ErrIncompatible) {
		t.Fatalf("esperava ErrIncompatible, veio %v", err)
	}
}

func TestCheckCompatibilityInputErrors(t *testing.T) {
	v1, _ := readingVersions(t)
	if err := contract.CheckCompatibility(contract.Backward, `{"type":"nope"}`, v1); err == nil {
		t.Error("candidato inválido deveria falhar")
	}
	if err := contract.CheckCompatibility("NONE_SUCH", v1, v1); err == nil {
		t.Error("nível desconhecido deveria falhar")
	}
	if err := contract.CheckCompatibility(contract.Backward, v1); err != nil {
		t.Errorf("primeira versão sem histórico deveria passar: %v", err)
	}
}
