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

// Cada serviço do profile secure monta só o diretório do próprio principal
// (nunca a raiz de .secure, o diretório da CA nem o de outro principal).
func TestSecureServicesMountOnlyTheirOwnPrincipalDirectory(t *testing.T) {
	root, _ := repoFiles(t)
	b, err := os.ReadFile(filepath.Join(root, "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"kafka-secure": "broker", "schema-registry-secure": "registry", "acls-secure": "ops", "init-secure": "ops",
		"simulator-secure": "simulator", "aggregator-secure": "aggregator", "notifier-secure": "notifier",
	}
	service := regexp.MustCompile(`^  ([a-z][a-z0-9-]*):\s*$`)
	mount := regexp.MustCompile(`^\s*-\s*"?\.?/?\.secure(/[^:"\s]*)?:`)
	var current string
	seen := map[string][]string{}
	for _, line := range strings.Split(string(b), "\n") {
		if m := service.FindStringSubmatch(line); m != nil {
			current = m[1]
			continue
		}
		if m := mount.FindStringSubmatch(line); m != nil {
			seen[current] = append(seen[current], strings.Trim(m[1], "/"))
		}
	}
	for svc, dirs := range seen {
		w, ok := want[svc]
		if !ok {
			t.Errorf("%s monta .secure mas não está na lista de serviços do profile", svc)
			continue
		}
		if len(dirs) != 1 || dirs[0] != w {
			t.Errorf("%s monta %v, só pode montar .secure/%s", svc, dirs, w)
		}
	}
	for svc := range want {
		if len(seen[svc]) == 0 {
			t.Errorf("%s não monta o diretório do principal", svc)
		}
	}
}

// secure-init.sh só apaga um diretório que ele mesmo criou (marcador) e nunca
// escreve num diretório alheio.
func TestSecureInitRefusesToTouchADirectoryItDidNotCreate(t *testing.T) {
	root, _ := repoFiles(t)
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("sem bash")
	}
	script := filepath.Join(root, "scripts", "secure-init.sh")
	victim := t.TempDir()
	sentinel := filepath.Join(victim, "precioso.txt")
	if err := os.WriteFile(sentinel, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"--force"}, {}} {
		cmd := exec.Command(bash, append([]string{script}, args...)...)
		cmd.Env = append(os.Environ(), "SECURE_DIR="+victim)
		if out, err := cmd.CombinedOutput(); err == nil {
			t.Fatalf("args %v: o script aceitou um diretório que não criou: %s", args, out)
		}
		if _, err := os.Stat(sentinel); err != nil {
			t.Fatalf("args %v: o script apagou o conteúdo do diretório alheio", args)
		}
	}
	// Caminho relativo é recusado antes de qualquer remoção. (Raiz e diretório
	// pessoal também são, mas um teste não deve nem chegar perto de apontar o
	// script para eles.)
	cmd := exec.Command(bash, script, "--force")
	cmd.Env = append(os.Environ(), "SECURE_DIR=relativo")
	if err := cmd.Run(); err == nil {
		t.Error("SECURE_DIR relativo aceito")
	}
}

// O Dockerfile da raiz faz COPY . .: sem estas entradas, o material de
// .secure (ca.key, senhas, JAAS) entraria no contexto, na camada do builder e
// no cache.
func TestDockerignoreKeepsSecretsOutOfTheBuildContext(t *testing.T) {
	root, _ := repoFiles(t)
	b, err := os.ReadFile(filepath.Join(root, ".dockerignore"))
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, l := range strings.Split(string(b), "\n") {
		have[strings.TrimSpace(l)] = true
	}
	for _, entry := range []string{".secure", ".env", "**/.env", "*.key", "*.p12", "*.pem", "*.password"} {
		if !have[entry] {
			t.Errorf(".dockerignore sem a entrada %q", entry)
		}
	}
}
