// Package hygiene guarda regras do repositório que não pertencem a nenhum
// pacote: nenhum segredo versionado.
package hygiene_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func repoFiles(t *testing.T) (root string, files []string) {
	t.Helper()
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Skip("sem git: a regra roda no CI")
	}
	root = strings.TrimSpace(string(out))
	list, err := exec.Command("git", "-C", root, "ls-files", "-z").Output()
	if err != nil {
		t.Skip("git ls-files falhou")
	}
	for _, f := range strings.Split(string(list), "\x00") {
		if f != "" && !strings.HasPrefix(f, "web/") {
			files = append(files, f)
		}
	}
	return root, files
}

// O material do profile secure é gerado por make secure-init e fica fora do git.
func TestNoPrivateKeyOrSecureMaterialIsVersioned(t *testing.T) {
	root, files := repoFiles(t)
	pemKey := regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`)
	forbidden := regexp.MustCompile(`(^|/)\.secure/|\.(key|p12|pfx|jks|keystore)$|\.password$`)
	for _, f := range files {
		if forbidden.MatchString(f) {
			t.Errorf("%s: material de segurança versionado", f)
			continue
		}
		b, err := os.ReadFile(filepath.Join(root, f))
		if err != nil || len(b) > 1<<20 {
			continue
		}
		if pemKey.Match(b) && !strings.HasSuffix(f, "_test.go") {
			t.Errorf("%s: chave privada versionada", f)
		}
	}
	gi, err := os.ReadFile(filepath.Join(root, ".gitignore"))
	if err != nil || !strings.Contains(string(gi), "/.secure/") {
		t.Error(".gitignore precisa ignorar /.secure/")
	}
}

// No compose, todo campo de senha ou segredo é referência (variável, arquivo)
// ou um valor marcado dev-only; nunca um literal que pareça real.
func TestComposeHasNoLiteralSecrets(t *testing.T) {
	root, _ := repoFiles(t)
	b, err := os.ReadFile(filepath.Join(root, "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	field := regexp.MustCompile(`(?i)^\s*[A-Z0-9_]*(PASSWORD|SECRET|CREDENTIALS)[A-Z0-9_]*:\s*(\S.*)$`)
	for i, line := range strings.Split(string(b), "\n") {
		m := field.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		v := strings.Trim(m[2], `"' `)
		if strings.HasPrefix(v, "${") || strings.HasPrefix(v, "/etc/") || strings.Contains(v, "dev-only") || strings.HasSuffix(strings.Split(m[0], ":")[0], "_FILE") || strings.HasSuffix(strings.Split(m[0], ":")[0], "_CREDENTIALS") {
			continue
		}
		t.Errorf("docker-compose.yml:%d: segredo literal: %s", i+1, strings.TrimSpace(line))
	}
}
