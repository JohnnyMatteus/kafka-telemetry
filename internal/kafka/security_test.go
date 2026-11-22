package kafka

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/config"
)

// pki gera, só para o teste, uma CA e um certificado de cliente assinado por ela.
func pki(t *testing.T, dir string) (ca, cert, key string) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	leafTpl := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "aggregator"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(leafKey)
	write := func(name, typ string, der []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	return write("ca.crt", "CERTIFICATE", caDER), write("client.crt", "CERTIFICATE", leafDER), write("client.key", "EC PRIVATE KEY", keyDER)
}

func TestSecurityValidateRefusesUnsafeCombinations(t *testing.T) {
	for name, s := range map[string]Security{
		"SASL sem TLS (senha em claro)": {Mechanism: "PLAIN", User: "u", Password: "p"},
		"SASL sem senha":                {Mechanism: "SCRAM-SHA-512", User: "u", UseTLS: true},
		"mecanismo desconhecido":        {Mechanism: "GSSAPI", User: "u", Password: "p", UseTLS: true},
		"certificado sem chave":         {UseTLS: true, CertFile: "c.pem"},
		"senha sem mecanismo":           {UseTLS: true, User: "u", Password: "p"},
	} {
		if err := s.Validate(); err == nil {
			t.Errorf("%s: esperava erro", name)
		}
	}
	if err := (Security{}).Validate(); err != nil {
		t.Errorf("sem segurança é o profile padrão: %v", err)
	}
	if (Security{}).Enabled() {
		t.Error("vazia não pode contar como ligada")
	}
}

func TestSecurityFromEnvReadsPasswordFromFileAndBuildsMutualTLS(t *testing.T) {
	dir := t.TempDir()
	ca, cert, key := pki(t, dir)
	pw := filepath.Join(dir, "password")
	if err := os.WriteFile(pw, []byte("dev-only-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KAFKA_TLS_CA_FILE", ca)
	t.Setenv("KAFKA_TLS_CERT_FILE", cert)
	t.Setenv("KAFKA_TLS_KEY_FILE", key)
	t.Setenv("KAFKA_SASL_MECHANISM", "scram-sha-512")
	t.Setenv("KAFKA_SASL_USER", "aggregator")
	t.Setenv("KAFKA_SASL_PASSWORD_FILE", pw)
	var env config.Env
	s, err := SecurityFromEnv(&env)
	if err != nil {
		t.Fatal(err)
	}
	if s.Password != "dev-only-secret" || s.Mechanism != "SCRAM-SHA-512" {
		t.Fatalf("security = %+v", s)
	}
	cfg, err := s.tlsConfig()
	if err != nil || len(cfg.Certificates) != 1 || cfg.RootCAs == nil {
		t.Fatalf("mTLS: certificados=%d CA=%v err=%v", len(cfg.Certificates), cfg.RootCAs != nil, err)
	}
	opts, err := s.Opts()
	if err != nil || len(opts) != 2 { // TLS + SASL
		t.Fatalf("opts=%d err=%v", len(opts), err)
	}
}

func TestSecurityRejectsMissingOrBrokenFiles(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "ca.crt")
	_ = os.WriteFile(bad, []byte("não é pem"), 0o600)
	if _, err := (Security{CAFile: bad}).tlsConfig(); err == nil {
		t.Error("CA inválida aceita")
	}
	t.Setenv("KAFKA_SASL_PASSWORD_FILE", filepath.Join(dir, "nao-existe"))
	var env config.Env
	if _, err := SecurityFromEnv(&env); err == nil {
		t.Error("arquivo de senha inexistente aceito")
	}
}

func TestRegistrySendsBasicAuthOverTLSAndRefusesItOverPlainHTTP(t *testing.T) {
	var gotUser, gotPass string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != "aggregator" || p != "dev-only-registry" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error_code":40101,"message":"Unauthorized"}`))
			return
		}
		gotUser, gotPass = u, p
		_, _ = w.Write([]byte(`{"schema":"\"string\""}`))
	}))
	defer srv.Close()
	dir := t.TempDir()
	caPath := filepath.Join(dir, "server-ca.crt")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}

	ok, err := NewRegistry(srv.URL, RegistrySecurity{User: "aggregator", Password: "dev-only-registry", CAFile: caPath})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ok.SchemaByID(t.Context(), 1); err != nil || gotUser != "aggregator" || gotPass != "dev-only-registry" {
		t.Fatalf("err=%v user=%q", err, gotUser)
	}
	// Sem credencial, o registry autenticado recusa.
	anon, err := NewRegistry(srv.URL, RegistrySecurity{CAFile: caPath})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := anon.SchemaByID(t.Context(), 1); err == nil {
		t.Fatal("o registry autenticado aceitou um cliente anônimo")
	}
	// Senha por http fora de loopback é recusada antes de sair da máquina.
	if _, err := NewRegistry("http://schema-registry:8081", RegistrySecurity{User: "u", Password: "p"}); err == nil {
		t.Fatal("senha por http aceita")
	}
	if _, err := NewRegistry("http://127.0.0.1:8081", RegistrySecurity{User: "u", Password: "p"}); err != nil {
		t.Fatalf("loopback deveria valer: %v", err)
	}
}

func TestRegistrySecurityFromEnvRequiresUserAndPasswordTogether(t *testing.T) {
	t.Setenv("SCHEMA_REGISTRY_USER", "aggregator")
	var env config.Env
	if _, err := RegistrySecurityFromEnv(&env); err == nil {
		t.Fatal("usuário sem senha aceito")
	}
}
