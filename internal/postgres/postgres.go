// Package postgres implementa as portas de leitura de séries e de alertas
// sobre as tabelas alimentadas pelos sinks JDBC.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store compartilha o pool entre os repositórios.
type Store struct {
	pool *pgxpool.Pool
}

// Open conecta e garante as colunas de reconhecimento.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close libera o pool.
func (s *Store) Close() { s.pool.Close() }

// Ping verifica a conexão.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// migrate é idempotente e cobre volumes criados antes do reconhecimento. O
// sink JDBC faz upsert só das colunas do registro, então não apaga estas.
func (s *Store) migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
ALTER TABLE greenhouse_alerts ADD COLUMN IF NOT EXISTS acknowledged_at TIMESTAMPTZ;
ALTER TABLE greenhouse_alerts ADD COLUMN IF NOT EXISTS note TEXT;`)
	if err != nil {
		return fmt.Errorf("postgres: migrar alertas: %w", err)
	}
	return nil
}

// Exec roda um comando avulso; existe para testes que simulam o sink JDBC.
func (s *Store) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := s.pool.Exec(ctx, sql, args...)
	return err
}
