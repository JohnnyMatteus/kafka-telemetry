// Package config lê configuração de variáveis de ambiente com valores
// padrão para desenvolvimento local. Erros de parse são acumulados e
// devolvidos juntos, para falhar cedo e com uma mensagem só.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env lê variáveis e acumula erros de conversão.
type Env struct {
	errs []error
}

// String devolve a variável ou o padrão.
func (e *Env) String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// List devolve a variável separada por vírgulas.
func (e *Env) List(key, def string) []string {
	var out []string
	for _, item := range strings.Split(e.String(key, def), ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// Int devolve a variável como inteiro.
func (e *Env) Int(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s=%q: inteiro inválido", key, v))
		return def
	}
	return n
}

// Float devolve a variável como número real.
func (e *Env) Float(key string, def float64) float64 {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s=%q: número inválido", key, v))
		return def
	}
	return f
}

// Duration devolve a variável como duração (ex.: 500ms, 1m).
func (e *Env) Duration(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s=%q: duração inválida", key, v))
		return def
	}
	return d
}

// Err devolve todos os erros acumulados.
func (e *Env) Err() error { return errors.Join(e.errs...) }
