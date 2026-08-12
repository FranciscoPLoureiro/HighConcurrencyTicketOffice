//go:build integration

// Integration tests run against a real PostgreSQL started by Testcontainers.
//
// There are no mocks here on purpose. A fake database agrees with whatever the
// code believes about isolation, locking and constraint enforcement, which is
// exactly the set of beliefs this project exists to test. The bugs being
// demonstrated only exist in a real engine.
package store

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// newTestStore starts a throwaway PostgreSQL, migrates it, and returns a Store
// pointed at it. The container is destroyed when the test ends.
func newTestStore(t *testing.T) *Store {
	t.Helper()

	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("tickets"),
		tcpostgres.WithUsername("tickets"),
		tcpostgres.WithPassword("tickets"),
		testcontainers.WithWaitStrategy(
			// Postgres starts, stops and restarts once while initialising,
			// so the readiness line appears twice. Waiting for the first
			// occurrence connects during the shutdown that follows it.
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("starting postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminating postgres: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("building connection string: %v", err)
	}

	store, err := Open(ctx, dsn, DefaultPoolConfig)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	t.Cleanup(store.Close)

	if _, err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrating: %v", err)
	}

	return store
}

func TestMigrationsApplyToAnEmptyDatabase(t *testing.T) {
	store := newTestStore(t)

	// Migrating twice must be a no-op rather than an error: every process
	// runs this at startup, and most of them find the work already done.
	version, err := store.Migrate(context.Background())
	if err != nil {
		t.Fatalf("second Migrate() = %v, want no error", err)
	}
	if version < 1 {
		t.Errorf("schema version = %d, want at least 1", version)
	}
}
