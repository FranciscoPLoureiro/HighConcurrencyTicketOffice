// Package store owns access to PostgreSQL, the system's source of truth.
//
// Every invariant that has to survive a restart is ultimately recoverable from
// here. Redis is a fast projection of this data, never the other way round.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is a handle on the connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// PoolConfig is how the connection pool is sized.
//
// This project uses pgxpool rather than database/sql, so the knobs are not the
// MaxOpenConns and MaxIdleConns of the standard library. They are these, and
// they do not mean quite the same things — MinConns is a floor pgxpool actively
// maintains, where database/sql's idle count is only a ceiling on what it
// keeps.
type PoolConfig struct {
	// MaxConns is the ceiling on connections this process holds.
	MaxConns int32
	// MinConns is the floor it keeps warm.
	MinConns int32
	// MaxConnLifetime retires a connection after this long regardless of
	// health, and MaxConnIdleTime retires one that has gone unused.
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	// HealthCheckPeriod is how often the pool inspects idle connections.
	HealthCheckPeriod time.Duration
}

// DefaultPoolConfig is the sizing this service runs with, and the reasoning is
// in the README as well as here because the brief asks for it explicitly.
//
// MaxConns is small on purpose. The instinct is to raise it under load, and it
// is the wrong instinct: PostgreSQL serves each connection with a backend
// process, so past the point where the connections outnumber what the machine
// can actually run concurrently, more of them means more context switching and
// more lock contention for exactly the same throughput. What matters here is
// that Redis has already refused everyone who was going to lose, so this pool
// serves a hundred inserts across a whole campaign rather than five thousand —
// and sixteen is generous for that plus the startup reconciliation's reads.
//
// MinConns keeps four warm because the campaign begins at midnight with no
// warning. A pool that dials on demand meets the burst with a handshake, a TLS
// negotiation and a round trip per connection, all inside the first requests —
// which is precisely the moment the latency is being measured.
//
// MaxConnLifetime is bounded so that connections are recycled past a load
// balancer or a failover that would otherwise leave this process talking to a
// former primary for as long as the socket stays open. Thirty minutes is long
// enough that recycling is invisible and short enough that a topology change is
// picked up without a restart. MaxConnIdleTime is much shorter: an idle
// connection is a backend process on the database doing nothing, and after a
// campaign there are a lot of them.
var DefaultPoolConfig = PoolConfig{
	MaxConns:          16,
	MinConns:          4,
	MaxConnLifetime:   30 * time.Minute,
	MaxConnIdleTime:   5 * time.Minute,
	HealthCheckPeriod: time.Minute,
}

// Open prepares a connection pool for the given DSN.
//
// It deliberately does not wait for a successful connection. pgxpool dials
// lazily, so a process whose database is briefly unavailable still starts,
// serves /health, and reports postgres as unavailable. The alternative —
// failing at startup — turns a thirty second database blip into a crash loop
// with no endpoint left to explain why.
//
// Every pool setting is applied explicitly rather than left at its default. The
// defaults are not unreasonable — MaxConns is four or the core count, whichever
// is larger — but they depend on the machine the process happens to land on,
// which means the pool is one size in CI and another in production and nobody
// chose either.
func Open(ctx context.Context, dsn string, pool PoolConfig) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}

	cfg.MaxConns = pool.MaxConns
	cfg.MinConns = pool.MinConns
	cfg.MaxConnLifetime = pool.MaxConnLifetime
	cfg.MaxConnIdleTime = pool.MaxConnIdleTime
	cfg.HealthCheckPeriod = pool.HealthCheckPeriod

	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("build postgres pool: %w", err)
	}

	return &Store{pool: p}, nil
}

// Ping reports whether the database is reachable and answering.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close releases every pooled connection.
func (s *Store) Close() {
	s.pool.Close()
}
