package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/config"
)

// Security é a configuração de TLS, mTLS e SASL dos clientes Kafka. Vazia, os
// clientes falam em texto claro e sem autenticação, como no profile padrão do
// compose; o profile secure a preenche (ADR 0016).
type Security struct {
	// CAFile liga o TLS e define em quem se confia. TLS liga sozinho (com as
	// raízes do sistema) se UseTLS for verdadeiro.
	CAFile string
	UseTLS bool
	// CertFile e KeyFile são o certificado do cliente (mTLS); vêm juntos.
	CertFile string
	KeyFile  string
	// Mechanism é PLAIN, SCRAM-SHA-256 ou SCRAM-SHA-512.
	Mechanism string
	User      string
	Password  string
}

// SecurityFromEnv lê KAFKA_TLS_ENABLED, KAFKA_TLS_CA_FILE, KAFKA_TLS_CERT_FILE,
// KAFKA_TLS_KEY_FILE, KAFKA_SASL_MECHANISM, KAFKA_SASL_USER e a senha, de
// preferência em arquivo (KAFKA_SASL_PASSWORD_FILE); KAFKA_SASL_PASSWORD
// também vale, para execuções avulsas.
func SecurityFromEnv(env *config.Env) (Security, error) {
	s := Security{
		UseTLS:    env.String("KAFKA_TLS_ENABLED", "") == "true",
		CAFile:    env.String("KAFKA_TLS_CA_FILE", ""),
		CertFile:  env.String("KAFKA_TLS_CERT_FILE", ""),
		KeyFile:   env.String("KAFKA_TLS_KEY_FILE", ""),
		Mechanism: strings.ToUpper(env.String("KAFKA_SASL_MECHANISM", "")),
		User:      env.String("KAFKA_SASL_USER", ""),
	}
	if err := env.Err(); err != nil {
		return s, err
	}
	pw, err := secretFromEnv(env, "KAFKA_SASL_PASSWORD")
	if err != nil {
		return s, err
	}
	s.Password = pw
	return s, s.Validate()
}

// secretFromEnv lê <nome>_FILE (conteúdo sem a quebra de linha final) ou <nome>.
func secretFromEnv(env *config.Env, name string) (string, error) {
	if path := env.String(name+"_FILE", ""); path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("%s_FILE: %w", name, err)
		}
		return strings.TrimRight(string(b), "\r\n"), nil
	}
	return env.String(name, ""), nil
}

// TLSEnabled informa se a conexão é cifrada.
func (s Security) TLSEnabled() bool { return s.UseTLS || s.CAFile != "" || s.CertFile != "" }

// Enabled informa se alguma proteção está ligada.
func (s Security) Enabled() bool { return s.TLSEnabled() || s.Mechanism != "" }

// Validate recusa as combinações que vazariam credencial ou ficariam pela metade.
func (s Security) Validate() error {
	if (s.CertFile == "") != (s.KeyFile == "") {
		return errors.New("kafka: KAFKA_TLS_CERT_FILE e KAFKA_TLS_KEY_FILE vêm juntos (mTLS)")
	}
	switch s.Mechanism {
	case "":
		if s.User != "" || s.Password != "" {
			return errors.New("kafka: usuário ou senha sem KAFKA_SASL_MECHANISM")
		}
		return nil
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
	default:
		return fmt.Errorf("kafka: KAFKA_SASL_MECHANISM %q desconhecido (PLAIN, SCRAM-SHA-256 ou SCRAM-SHA-512)", s.Mechanism)
	}
	if s.User == "" || s.Password == "" {
		return errors.New("kafka: SASL exige usuário e senha")
	}
	if !s.TLSEnabled() {
		return errors.New("kafka: SASL sem TLS mandaria a senha em texto claro; configure KAFKA_TLS_CA_FILE")
	}
	return nil
}

func (s Security) tlsConfig() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if s.CAFile != "" {
		pool, err := loadPool(s.CAFile)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	if s.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(s.CertFile, s.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("kafka: certificado do cliente: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func loadPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("kafka: CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("kafka: %s não contém certificado PEM", path)
	}
	return pool, nil
}

// Opts devolve as opções franz-go; vazias se nada está ligado. Todo cliente do
// pipeline (consumidor, produtor, admin, inspeção, checkpoint) as recebe.
func (s Security) Opts() ([]kgo.Opt, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	var opts []kgo.Opt
	if s.TLSEnabled() {
		cfg, err := s.tlsConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(cfg))
	}
	switch s.Mechanism {
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: s.User, Pass: s.Password}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: s.User, Pass: s.Password}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: s.User, Pass: s.Password}.AsSha512Mechanism()))
	}
	return opts, nil
}

// RegistrySecurity é a autenticação do Schema Registry (HTTP basic) e a CA em
// que o cliente confia.
type RegistrySecurity struct {
	User     string
	Password string
	CAFile   string
}

// RegistrySecurityFromEnv lê SCHEMA_REGISTRY_USER, a senha (arquivo
// SCHEMA_REGISTRY_PASSWORD_FILE ou SCHEMA_REGISTRY_PASSWORD) e SCHEMA_REGISTRY_CA_FILE.
func RegistrySecurityFromEnv(env *config.Env) (RegistrySecurity, error) {
	s := RegistrySecurity{User: env.String("SCHEMA_REGISTRY_USER", ""), CAFile: env.String("SCHEMA_REGISTRY_CA_FILE", "")}
	if err := env.Err(); err != nil {
		return s, err
	}
	pw, err := secretFromEnv(env, "SCHEMA_REGISTRY_PASSWORD")
	if err != nil {
		return s, err
	}
	s.Password = pw
	if (s.User == "") != (s.Password == "") {
		return s, errors.New("schema registry: usuário e senha vêm juntos")
	}
	return s, nil
}

// clientOpts valida contra a URL e monta as opções do cliente do registry.
// Credencial só segue por https, ou por loopback.
func (s RegistrySecurity) clientOpts(registryURL string) ([]sr.ClientOpt, error) {
	u, err := url.Parse(registryURL)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("schema registry: URL inválida %q", registryURL)
	}
	if s.User != "" && u.Scheme != "https" && !isLoopback(u.Hostname()) {
		return nil, errors.New("schema registry: a senha só segue por https (ou loopback); use uma URL https e SCHEMA_REGISTRY_CA_FILE")
	}
	var opts []sr.ClientOpt
	if s.CAFile != "" {
		pool, err := loadPool(s.CAFile)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sr.HTTPClient(&http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}},
		}))
	}
	if s.User != "" {
		opts = append(opts, sr.BasicAuth(s.User, s.Password))
	}
	return opts, nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
