package store

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrationLockID identifies the advisory lock that serialises schema changes.
// The value is arbitrary; only its uniqueness within this database matters.
const migrationLockID int64 = 8412207

// Migrate applies every pending migration and reports the resulting version.
//
// Migrations are append-only. A migration that has shipped is never edited,
// because the next environment to run it would get a different schema from the
// one already in production while both report the same version.
func (s *Store) Migrate(ctx context.Context) (int64, error) {
	db := stdlib.OpenDBFromPool(s.pool)
	defer func() { _ = db.Close() }()

	// Instances routinely start together — a rolling deploy guarantees it —
	// and would otherwise race to apply the same migration. Advisory locks
	// live for the lifetime of a session, so the lock and its release have
	// to run on one pinned connection; a pooled Exec could easily unlock
	// from a different session and release nothing.
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire migration connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", migrationLockID); err != nil {
		return 0, fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		// WithoutCancel: if the caller's context has already expired, the
		// lock still has to come off. Leaving it held would block every
		// future deployment until this session happens to end.
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", migrationLockID)
	}()

	goose.SetBaseFS(migrationFS)
	// goose logs to stdout by default, which would interleave unstructured
	// text with the JSON the rest of the process emits.
	goose.SetLogger(goose.NopLogger())

	if err := goose.SetDialect("postgres"); err != nil {
		return 0, fmt.Errorf("set migration dialect: %w", err)
	}
	if err := goose.UpContext(ctx, db, "migrations"); err != nil {
		return 0, fmt.Errorf("apply migrations: %w", err)
	}

	version, err := goose.GetDBVersionContext(ctx, db)
	if err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	return version, nil
}
