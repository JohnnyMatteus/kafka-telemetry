package hygiene_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Em bash com `set -e`, um comando negado (`! cmd`) que não é o último do
// passo nunca derruba o passo: a asserção fica morta e passa sempre. Os
// workflows e os scripts usam `if cmd; then exit 1; fi`.
func TestNoDeadNegatedAssertionsInWorkflowsAndScripts(t *testing.T) {
	root, _ := repoFiles(t)
	var files []string
	for _, pat := range []string{".github/workflows/*.yml", "scripts/*.sh", "deploy/secure/*.sh"} {
		m, err := filepath.Glob(filepath.Join(root, pat))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, m...)
	}
	negated := regexp.MustCompile(`^\s*!\s+\S`)
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			if negated.MatchString(line) {
				t.Errorf("%s:%d: asserção negada com `!` não derruba o passo sob set -e; use if ...; then exit 1; fi: %s",
					filepath.Base(f), i+1, strings.TrimSpace(line))
			}
		}
	}
}

// Todo Deployment do chart declara usuário numérico: a imagem usa USER por nome
// e o kubelet recusa runAsNonRoot sem UID numérico.
func TestHelmValuesPinANumericUser(t *testing.T) {
	root, _ := repoFiles(t)
	b, err := os.ReadFile(filepath.Join(root, "deploy", "helm", "kafka-telemetry", "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"runAsNonRoot: true", "runAsUser: 65532", "runAsGroup: 65532"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("values.yaml sem %q", want)
		}
	}
}
