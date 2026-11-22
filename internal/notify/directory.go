package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"os"
	"regexp"
	"strings"
)

// FileDirectory é o diretório de produtores lido de um arquivo JSON:
//
//	{"producers":[{"id":"p1","name":"Ana","email":"ana@example.com",
//	               "webhook_url":"https://...","webhook_secret_env":"HOOK_ANA",
//	               "greenhouses":["gh-001"]}]}
//
// O arquivo é de operação (montado no deploy), não é dado de usuário.
type FileDirectory struct {
	webhooks     bool
	byGreenhouse map[string][]Recipient
}

type producerFile struct {
	Producers []struct {
		ID         string `json:"id"`
		Name       string `json:"name"`
		Email      string `json:"email"`
		WebhookURL string `json:"webhook_url"`
		// SecretEnv é o NOME da variável de ambiente com o segredo do webhook
		// deste produtor; o segredo nunca fica no arquivo.
		SecretEnv   string   `json:"webhook_secret_env"`
		Greenhouses []string `json:"greenhouses"`
	} `json:"producers"`
}

// LoadDirectory lê e valida o arquivo.
func LoadDirectory(path string) (*FileDirectory, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("diretório de produtores: %w", err)
	}
	return ParseDirectory(b, os.LookupEnv)
}

var envName = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

// HasWebhooks informa se algum produtor usa webhook.
func (d *FileDirectory) HasWebhooks() bool { return d.webhooks }

// ParseDirectory valida o conteúdo: id único, ao menos um contato e uma
// estufa por produtor, e-mail bem formado.
func ParseDirectory(b []byte, lookupEnv func(string) (string, bool)) (*FileDirectory, error) {
	var f producerFile
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("diretório de produtores: %w", err)
	}
	if len(f.Producers) == 0 {
		return nil, errors.New("diretório de produtores vazio")
	}
	d := &FileDirectory{byGreenhouse: map[string][]Recipient{}}
	seen := map[string]bool{}
	for _, p := range f.Producers {
		switch {
		case p.ID == "" || strings.ContainsAny(p.ID, ": \r\n"):
			return nil, fmt.Errorf("produtor com id inválido %q", p.ID)
		case seen[p.ID]:
			return nil, fmt.Errorf("produtor %q repetido", p.ID)
		case p.Email == "" && p.WebhookURL == "":
			return nil, fmt.Errorf("produtor %q sem e-mail nem webhook", p.ID)
		case len(p.Greenhouses) == 0:
			return nil, fmt.Errorf("produtor %q sem estufas", p.ID)
		}
		if p.Email != "" {
			if _, err := mail.ParseAddress(p.Email); err != nil || strings.ContainsAny(p.Email, "\r\n") {
				return nil, fmt.Errorf("produtor %q: e-mail inválido", p.ID)
			}
		}
		seen[p.ID] = true
		r := Recipient{ID: p.ID, Name: p.Name, Email: p.Email, WebhookURL: p.WebhookURL}
		if p.WebhookURL != "" {
			if !envName.MatchString(p.SecretEnv) {
				return nil, fmt.Errorf("produtor %q: webhook_secret_env deve nomear uma variável de ambiente (MAIUSCULAS_E_SUBLINHADO)", p.ID)
			}
			secret, ok := lookupEnv(p.SecretEnv)
			if !ok || len(secret) < MinSecretLen {
				return nil, fmt.Errorf("produtor %q: a variável %s não existe ou tem menos de %d caracteres", p.ID, p.SecretEnv, MinSecretLen)
			}
			r.WebhookSecret = secret
			d.webhooks = true
		} else if p.SecretEnv != "" {
			return nil, fmt.Errorf("produtor %q: webhook_secret_env sem webhook_url", p.ID)
		}
		for _, gh := range p.Greenhouses {
			d.byGreenhouse[gh] = append(d.byGreenhouse[gh], r)
		}
	}
	return d, nil
}

func (d *FileDirectory) Lookup(_ context.Context, greenhouseID string) ([]Recipient, error) {
	return d.byGreenhouse[greenhouseID], nil
}
