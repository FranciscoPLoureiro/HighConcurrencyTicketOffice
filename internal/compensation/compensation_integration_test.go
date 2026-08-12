//go:build integration

// The compensation saga, against a real PostgreSQL and a real Redis.
//
// The question these ask is not whether a reversal works — that is three
// statements — but whether it survives being run more than once. The broker
// delivers at least once, so this code will be handed the same failed sale
// twice sooner or later, and the brief is explicit about what a careless
// version does: "if the compensation runs twice, the stock increments twice and
// you now have 101 tickets".
package compensation

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/cache"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/queue"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/store"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

const testCampaign = "queima-test"

// recordingNotifier stands in for the broker. What it publishes is not the
// interesting part of these tests; that it is published *after* the reversal,
// and that a failure to publish does not undo one, is.
type recordingNotifier struct {
	mu   sync.Mutex
	sent []queue.TicketMessage
	err  error
}

func (n *recordingNotifier) Compensate(_ context.Context, message queue.TicketMessage) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.err != nil {
		return n.err
	}
	n.sent = append(n.sent, message)
	return nil
}

func (n *recordingNotifier) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.sent)
}

type harness struct {
	store    *store.Store
	cache    *cache.Cache
	notifier *recordingNotifier
	service  *Service
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	db := startPostgres(t)
	redis := startRedis(t)
	notifier := &recordingNotifier{}

	ctx := context.Background()
	if err := db.EnsureCampaign(ctx, testCampaign, 10); err != nil {
		t.Fatalf("EnsureCampaign() = %v", err)
	}
	if err := redis.Reconcile(ctx, testCampaign, 10, nil); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	return &harness{
		store:    db,
		cache:    redis,
		notifier: notifier,
		service:  New(db, redis, notifier, 5*time.Second, slog.New(slog.DiscardHandler)),
	}
}

// sell puts a ticket in the state a failed fulfilment leaves it: sold in Redis,
// recorded in PostgreSQL, and never going to be completed.
func (h *harness) sell(t *testing.T, userID string) domain.Purchase {
	t.Helper()

	ctx := context.Background()
	outcome, _, err := h.cache.Purchase(ctx, testCampaign, userID)
	if err != nil || outcome != cache.Sold {
		t.Fatalf("cache.Purchase() = %v, %v, want Sold", outcome, err)
	}

	purchase, err := h.store.RecordPending(ctx, testCampaign, userID, uuid.NewString())
	if err != nil {
		t.Fatalf("RecordPending() = %v", err)
	}
	return purchase
}

func (h *harness) delivery(p domain.Purchase) queue.Delivery {
	return queue.Delivery{Message: queue.TicketMessage{
		PurchaseID:     p.ID,
		CampaignID:     p.CampaignID,
		UserID:         p.UserID,
		IdempotencyKey: p.IdempotencyKey,
		CorrelationID:  "correlation-under-test",
		IssuedAt:       time.Now().UTC(),
	}}
}

func (h *harness) stock(t *testing.T) int64 {
	t.Helper()

	remaining, _, err := h.cache.Remaining(context.Background(), testCampaign)
	if err != nil {
		t.Fatalf("Remaining() = %v", err)
	}
	return remaining
}

// The saga, once: the row is cancelled, the ticket is back, the buyer is free.
func TestCompensationReversesTheSaleCompletely(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	purchase := h.sell(t, "student-1")
	if got := h.stock(t); got != 9 {
		t.Fatalf("stock is %d after the sale, want 9", got)
	}

	if err := h.service.Compensate(ctx, h.delivery(purchase)); err != nil {
		t.Fatalf("Compensate() = %v", err)
	}

	if got := h.stock(t); got != 10 {
		t.Errorf("stock is %d after the reversal, want 10", got)
	}

	settled, err := h.store.ReadPurchase(ctx, testCampaign, purchase.ID)
	if err != nil {
		t.Fatalf("ReadPurchase() = %v", err)
	}
	if settled.Status != domain.StatusCancelled {
		t.Errorf("purchase is %s, want %s", settled.Status, domain.StatusCancelled)
	}

	// The half that has no symptom when it is missing: the buyer must be out
	// of the holders set, or they can never buy again in this campaign.
	held, err := h.cache.HoldsTicket(ctx, testCampaign, "student-1")
	if err != nil {
		t.Fatalf("HoldsTicket() = %v", err)
	}
	if held {
		t.Error("redis still has student-1 down as a holder; they can never buy again")
	}

	if h.notifier.count() != 1 {
		t.Errorf("published %d compensation notices, want 1", h.notifier.count())
	}
}

// The bug the brief says an interviewer will look for.
//
// At-least-once delivery guarantees this message arrives twice eventually, and
// a careless saga credits the stock twice — a campaign of ten tickets quietly
// becoming eleven.
func TestCompensatingTwiceReturnsOneTicket(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	purchase := h.sell(t, "student-1")

	for i := range 3 {
		if err := h.service.Compensate(ctx, h.delivery(purchase)); err != nil {
			t.Fatalf("Compensate() attempt %d = %v", i+1, err)
		}
	}

	if got := h.stock(t); got != 10 {
		t.Errorf("stock is %d after compensating the same sale three times, want 10", got)
	}

	live, err := h.store.CountLiveTickets(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountLiveTickets() = %v", err)
	}
	if live != 0 {
		t.Errorf("postgres holds %d live tickets, want 0", live)
	}
}

// The same guarantee under a race rather than in sequence, which is how a
// redelivery to two workers actually arrives.
func TestConcurrentCompensationsReturnOneTicket(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	purchase := h.sell(t, "student-1")

	const attempts = 20
	start := make(chan struct{})
	var wg sync.WaitGroup

	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := h.service.Compensate(ctx, h.delivery(purchase)); err != nil {
				t.Errorf("Compensate() = %v", err)
			}
		}()
	}

	close(start)
	wg.Wait()

	if got := h.stock(t); got != 10 {
		t.Errorf("stock is %d after %d simultaneous compensations, want 10", got, attempts)
	}
}

// A compensated buyer can buy again, which is the whole reason the reversal
// removes them from the holders set rather than only cancelling the row.
func TestACompensatedBuyerCanBuyAgain(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	purchase := h.sell(t, "student-1")
	if err := h.service.Compensate(ctx, h.delivery(purchase)); err != nil {
		t.Fatalf("Compensate() = %v", err)
	}

	outcome, _, err := h.cache.Purchase(ctx, testCampaign, "student-1")
	if err != nil {
		t.Fatalf("Purchase() after compensation = %v", err)
	}
	if outcome != cache.Sold {
		t.Errorf("redis refused a compensated buyer with outcome %d, want Sold", outcome)
	}

	if _, err := h.store.RecordPending(ctx, testCampaign, "student-1", uuid.NewString()); err != nil {
		t.Errorf("postgres refused a compensated buyer: %v", err)
	}
}

// A notification that cannot be published does not undo a correct reversal.
//
// The ticket is back on the shelf and the row is cancelled; those are the parts
// that matter. Returning an error here would have the message redelivered and
// the whole saga run again for the sake of a log line.
func TestAFailedNotificationDoesNotUndoTheReversal(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	purchase := h.sell(t, "student-1")
	h.notifier.err = errNotifierUnavailable

	if err := h.service.Compensate(ctx, h.delivery(purchase)); err != nil {
		t.Fatalf("Compensate() = %v, want the reversal to stand", err)
	}

	if got := h.stock(t); got != 10 {
		t.Errorf("stock is %d, want 10 — the reversal must not depend on the notice", got)
	}
}

// A message naming a purchase that does not exist is dropped rather than
// retried, or it comes back forever.
func TestCompensatingAPurchaseThatDoesNotExistIsNotAnError(t *testing.T) {
	h := newHarness(t)

	orphan := h.delivery(domain.Purchase{
		ID:         uuid.NewString(),
		CampaignID: testCampaign,
		UserID:     "student-1",
	})

	if err := h.service.Compensate(context.Background(), orphan); err != nil {
		t.Errorf("Compensate() on a missing purchase = %v, want it dropped", err)
	}
}

var errNotifierUnavailable = &notifierError{}

type notifierError struct{}

func (e *notifierError) Error() string { return "broker unreachable" }

func startPostgres(t *testing.T) *store.Store {
	t.Helper()

	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("tickets"),
		tcpostgres.WithUsername("tickets"),
		tcpostgres.WithPassword("tickets"),
		testcontainers.WithWaitStrategy(
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

func startRedis(t *testing.T) *cache.Cache {
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

	c := cache.Open(net.JoinHostPort(host, port.Port()), "")
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Logf("closing redis: %v", err)
		}
	})

	return c
}
