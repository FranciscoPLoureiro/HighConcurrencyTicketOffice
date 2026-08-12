//go:build integration

// Integration tests for the phase 2 purchase path, against a real PostgreSQL
// and a real Redis started by Testcontainers.
//
// Nothing here is mocked, and that is the point. A fake Redis agrees with
// whatever the code believes about atomicity, and atomicity is the only thing
// being tested. The Lua script either does hold under five hundred concurrent
// callers on a real single-threaded server, or it does not, and no test double
// can tell the difference.
package purchase

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/cache"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/store"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

const testCampaign = "queima-test"

// harness is one throwaway system: a Postgres, a Redis, and the service that
// joins them.
type harness struct {
	service *Service
	store   *store.Store
	cache   *cache.Cache
	// redisAddr lets a test build a second, independent client — which is
	// how a process restart is simulated without restarting anything.
	redisAddr string
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	db := startPostgres(t)
	addr := startRedis(t)
	redis := openCache(t, addr)

	return &harness{
		service:   New(redis, db, slog.New(slog.DiscardHandler)),
		store:     db,
		cache:     redis,
		redisAddr: addr,
	}
}

// openCampaign creates a campaign of the given size and reconciles Redis from
// it, which is what startup does.
func (h *harness) openCampaign(t *testing.T, total int) {
	t.Helper()

	ctx := context.Background()
	if err := h.store.EnsureCampaign(ctx, testCampaign, total); err != nil {
		t.Fatalf("EnsureCampaign() = %v", err)
	}
	if _, err := h.service.Reconcile(ctx, testCampaign); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}
}

// restart returns a service backed by fresh connections to the same Postgres
// and the same Redis.
//
// This is what a deploy, a crash or an OOM kill leaves behind: the data is
// still there, the process is not. Everything the old process held in memory is
// gone, and the new one has to work out what happened from what it can read.
func (h *harness) restart(t *testing.T) *Service {
	t.Helper()

	return New(openCache(t, h.redisAddr), h.store, slog.New(slog.DiscardHandler))
}

func startPostgres(t *testing.T) *store.Store {
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

	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	t.Cleanup(db.Close)

	if _, err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrating: %v", err)
	}

	return db
}

func startRedis(t *testing.T) string {
	t.Helper()

	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:8-alpine")
	if err != nil {
		t.Fatalf("starting redis: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminating redis: %v", err)
		}
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("reading redis host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatalf("reading redis port: %v", err)
	}

	return net.JoinHostPort(host, port.Port())
}

func openCache(t *testing.T, addr string) *cache.Cache {
	t.Helper()

	c := cache.Open(addr, "")
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Logf("closing redis client: %v", err)
		}
	})
	return c
}

// student produces a distinct identity per contender, so that a refusal for
// holding a ticket already is a real signal rather than the test reusing one
// account.
func student(i int) string { return "student-" + strconv.Itoa(i) }
