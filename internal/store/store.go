// Package store owns access to PostgreSQL, the system's source of truth.
//
// Every invariant that has to survive a restart is ultimately recoverable from
// here. Redis is a fast projection of this data, never the other way round.
package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is a handle on the connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// Open prepares a connection pool for the given DSN.
//
// It deliberately does not wait for a successful connection. pgxpool dials
// lazily, so a process whose database is briefly unavailable still starts,
// serves /health, and reports postgres as unavailable. The alternative —
// failing at startup — turns a thirty second database blip into a crash loop
// with no endpoint left to explain why.
func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("build postgres pool: %w", err)
	}

	return &Store{pool: pool}, nil
}

// Ping reports whether the database is reachable and answering.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close releases every pooled connection.
func (s *Store) Close() {
	s.pool.Close()
}
