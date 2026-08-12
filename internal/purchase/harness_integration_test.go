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
	"sync"
	"testing"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/cache"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/queue"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/store"
	"github.com/google/uuid"
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
	// published collects every ticket the service handed over, which is how
	// a test asserts that a sale was actually queued rather than only
	// recorded.
	published *recordingFulfiller
	// redisAddr lets a test build a second, independent client — which is
	// how a process restart is simulated without restarting anything.
	redisAddr string
	// redisContainer lets a test take Redis away mid-flight. The brief asks
	// for exactly this: Testcontainers can stop a container during a test,
	// and the behaviour when the one system enforcing the stock invariant
	// disappears is worth proving rather than asserting.
	redisContainer testcontainers.Container
}

// recordingFulfiller stands in for RabbitMQ.
//
// These tests are about the two systems that decide and remember; the broker
// gets its own suite against a real one in internal/queue, where atomicity and
// confirms are the point. Here it only has to record what it was given and, on
// request, fail in a specified way so the compensation paths can be reached.
type recordingFulfiller struct {
	mu   sync.Mutex
	sent []queue.TicketMessage
	err  error
}

func (f *recordingFulfiller) PublishTicket(_ context.Context, message queue.TicketMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, message)
	return nil
}

func (f *recordingFulfiller) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

func (f *recordingFulfiller) messages() []queue.TicketMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]queue.TicketMessage(nil), f.sent...)
}

func (f *recordingFulfiller) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	db := startPostgres(t)
	addr, container := startRedis(t)
	redis := openCache(t, addr)
	published := &recordingFulfiller{}

	return &harness{
		service:        New(redis, db, published, Timeouts{}, slog.New(slog.DiscardHandler)),
		store:          db,
		cache:          redis,
		published:      published,
		redisAddr:      addr,
		redisContainer: container,
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

	return New(openCache(t, h.redisAddr), h.store, h.published, Timeouts{}, slog.New(slog.DiscardHandler))
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

	db, err := store.Open(ctx, dsn, store.DefaultPoolConfig)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	t.Cleanup(db.Close)

	if _, err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrating: %v", err)
	}

	return db
}

func startRedis(t *testing.T) (string, testcontainers.Container) {
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

	return net.JoinHostPort(host, port.Port()), container
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

// newKey produces a fresh idempotency key, which is what a client that has not
// retried anything sends. Tests about retrying send the same one twice on
// purpose; everywhere else a new key per attempt is the honest default, because
// reusing one would silently make half these tests replays.
func newKey() string { return uuid.NewString() }
