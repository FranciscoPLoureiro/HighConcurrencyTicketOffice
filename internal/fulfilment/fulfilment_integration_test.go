//go:build integration

// The end of the phase 3 loop, against a real PostgreSQL and a real RabbitMQ.
//
// Everything up to here has been tested a piece at a time: the Lua script holds
// under contention, the publisher knows whether the broker took a message, the
// consumer retries and gives up in the right places. This is the only test that
// puts a message in one end and looks for a confirmed ticket at the other, and
// it is the one that would catch two correct halves wired together wrongly.
package fulfilment

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/queue"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/store"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"
)

const testCampaign = "queima-test"

type harness struct {
	store     *store.Store
	broker    *queue.Connection
	publisher *queue.Publisher
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	db := startPostgres(t)
	broker := startBroker(t)

	publisher, err := queue.NewPublisher(broker, 2)
	if err != nil {
		t.Fatalf("NewPublisher() = %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Logf("closing publisher: %v", err)
		}
	})

	ctx := context.Background()
	if err := db.EnsureCampaign(ctx, testCampaign, 100); err != nil {
		t.Fatalf("EnsureCampaign() = %v", err)
	}

	return &harness{store: db, broker: broker, publisher: publisher}
}

// sell writes a pending purchase the way the API does, without going through
// Redis: this suite is about what happens after the sale, and the sale itself
// has its own tests.
func (h *harness) sell(t *testing.T, userID string) domain.Purchase {
	t.Helper()

	purchase, err := h.store.RecordPending(context.Background(), testCampaign, userID, uuid.NewString())
	if err != nil {
		t.Fatalf("RecordPending() = %v", err)
	}
	return purchase
}

func (h *harness) message(p domain.Purchase) queue.TicketMessage {
	return queue.TicketMessage{
		PurchaseID:     p.ID,
		CampaignID:     p.CampaignID,
		UserID:         p.UserID,
		IdempotencyKey: p.IdempotencyKey,
		CorrelationID:  "correlation-under-test",
		IssuedAt:       time.Now().UTC(),
	}
}

// runWorker starts the real consumer with the real fulfilment handler, wired
// exactly as cmd/worker wires them.
func (h *harness) runWorker(t *testing.T, delay time.Duration, observe func(queue.Delivery)) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	work := New(h.store, delay, 5*time.Second, slog.New(slog.DiscardHandler))
	consumer := queue.NewConsumer(h.broker, h.publisher, 4, 30*time.Second, slog.New(slog.DiscardHandler))

	handler := func(ctx context.Context, d queue.Delivery) error {
		if observe != nil {
			observe(d)
		}
		return work.Fulfil(ctx, d)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Consume(ctx, handler); err != nil && ctx.Err() == nil {
			t.Errorf("Consume() = %v", err)
		}
	}()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			wg.Wait()
		})
	}
	t.Cleanup(stop)
	return stop
}

// awaitStatus polls until the purchase reaches the expected state.
func (h *harness) awaitStatus(t *testing.T, purchaseID string, want domain.Status, within time.Duration) domain.Purchase {
	t.Helper()

	deadline := time.Now().Add(within)
	var last domain.Purchase

	for time.Now().Before(deadline) {
		got, err := h.store.ReadPurchase(context.Background(), testCampaign, purchaseID)
		if err != nil {
			t.Fatalf("ReadPurchase() = %v", err)
		}
		last = got
		if got.Status == want {
			return got
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("purchase %s is %s after %s, want %s", purchaseID, last.Status, within, want)
	return last
}

// The loop this phase exists to build: sold here, finished over there.
func TestAPublishedTicketIsFulfilledAndConfirmed(t *testing.T) {
	h := newHarness(t)
	h.runWorker(t, 0, nil)

	purchase := h.sell(t, "student-1")
	if purchase.Status != domain.StatusPending {
		t.Fatalf("a new purchase is %s, want %s", purchase.Status, domain.StatusPending)
	}

	if err := h.publisher.PublishTicket(context.Background(), h.message(purchase)); err != nil {
		t.Fatalf("PublishTicket() = %v", err)
	}

	confirmed := h.awaitStatus(t, purchase.ID, domain.StatusConfirmed, 30*time.Second)
	if confirmed.UpdatedAt.Before(confirmed.CreatedAt) {
		t.Error("updated_at went backwards when the status changed")
	}
}

// The requirement at-least-once delivery imposes on everybody downstream of it.
//
// The same message twice must produce one confirmed ticket, not two of
// anything. No broker setting prevents the duplicate, so the only place this
// can be guaranteed is here.
func TestTheSameMessageDeliveredTwiceConfirmsOneTicket(t *testing.T) {
	h := newHarness(t)

	var deliveries atomic.Int64
	h.runWorker(t, 0, func(queue.Delivery) { deliveries.Add(1) })

	purchase := h.sell(t, "student-1")
	message := h.message(purchase)

	for range 2 {
		if err := h.publisher.PublishTicket(context.Background(), message); err != nil {
			t.Fatalf("PublishTicket() = %v", err)
		}
	}

	h.awaitStatus(t, purchase.ID, domain.StatusConfirmed, 30*time.Second)

	// Both copies must actually have been delivered, or this test passed by
	// accident rather than by anything being idempotent.
	deadline := time.Now().Add(15 * time.Second)
	for deliveries.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if got := deliveries.Load(); got != 2 {
		t.Fatalf("the handler saw %d deliveries, want 2 — the duplicate never arrived", got)
	}

	// One ticket, still. The second delivery found nothing to do and said so
	// rather than failing, which is what lets the consumer acknowledge it.
	live, err := h.store.CountLiveTickets(context.Background(), testCampaign)
	if err != nil {
		t.Fatalf("CountLiveTickets() = %v", err)
	}
	if live != 1 {
		t.Errorf("%d live tickets after a duplicate delivery, want 1", live)
	}

	// And nothing was retried into the dead letter queue on the way.
	if depth := queueDepth(t, h.broker, queue.DeadLetterQueue); depth != 0 {
		t.Errorf("%d messages reached the dead letter queue, want 0", depth)
	}
}

// A message naming a purchase that does not exist is not worth three attempts.
//
// It fails identically every time, so retrying spends thirty-five seconds
// proving what the first attempt already knew, and spends it in a queue
// somebody is watching for real problems.
func TestAMessageForAPurchaseThatDoesNotExistIsNotRetried(t *testing.T) {
	h := newHarness(t)

	var attempts atomic.Int64
	h.runWorker(t, 0, func(queue.Delivery) { attempts.Add(1) })

	orphan := h.message(domain.Purchase{
		ID:         uuid.NewString(),
		CampaignID: testCampaign,
		UserID:     "student-1",
	})
	if err := h.publisher.PublishTicket(context.Background(), orphan); err != nil {
		t.Fatalf("PublishTicket() = %v", err)
	}

	if depth := waitForQueueDepth(t, h.broker, queue.DeadLetterQueue, 1, 30*time.Second); depth != 1 {
		t.Fatalf("the dead letter queue holds %d messages, want 1", depth)
	}

	// The point of the exercise: it got there on the first attempt.
	if got := attempts.Load(); got != 1 {
		t.Errorf("the handler ran %d times for a permanent failure, want 1", got)
	}
}

// A cancelled purchase is not confirmed by a message that predates the
// cancellation.
//
// Redelivery is expected and the queue offers no ordering guarantee against
// decisions taken elsewhere, so a stale message must not resurrect a ticket
// that has already gone back into the pool.
func TestACancelledPurchaseIsNotConfirmedByAStaleMessage(t *testing.T) {
	h := newHarness(t)
	h.runWorker(t, 0, nil)

	purchase := h.sell(t, "student-1")
	if _, err := h.store.CancelPurchase(context.Background(), purchase.ID); err != nil {
		t.Fatalf("CancelPurchase() = %v", err)
	}

	if err := h.publisher.PublishTicket(context.Background(), h.message(purchase)); err != nil {
		t.Fatalf("PublishTicket() = %v", err)
	}

	if depth := waitForQueueDepth(t, h.broker, queue.DeadLetterQueue, 1, 30*time.Second); depth != 1 {
		t.Fatalf("the dead letter queue holds %d messages, want 1", depth)
	}

	got, err := h.store.ReadPurchase(context.Background(), testCampaign, purchase.ID)
	if err != nil {
		t.Fatalf("ReadPurchase() = %v", err)
	}
	if got.Status != domain.StatusCancelled {
		t.Errorf("a cancelled purchase became %s, want it left alone", got.Status)
	}
}

// Shutdown finishes the message in hand rather than dropping it.
//
// The brief asks for exactly this, and getting it wrong is invisible until a
// deploy leaves a scattering of half-fulfilled tickets behind it.
func TestShutdownFinishesTheMessageItIsHolding(t *testing.T) {
	h := newHarness(t)

	started := make(chan struct{})
	var once sync.Once

	// A delay long enough that the stop below lands squarely in the middle of
	// the fulfilment rather than before or after it.
	stop := h.runWorker(t, 2*time.Second, func(queue.Delivery) {
		once.Do(func() { close(started) })
	})

	purchase := h.sell(t, "student-1")
	if err := h.publisher.PublishTicket(context.Background(), h.message(purchase)); err != nil {
		t.Fatalf("PublishTicket() = %v", err)
	}

	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("the handler never started")
	}

	stop()

	// Not awaited. By the time Consume has returned, the message it was
	// working on is finished and acknowledged, or the guarantee is broken.
	got, err := h.store.ReadPurchase(context.Background(), testCampaign, purchase.ID)
	if err != nil {
		t.Fatalf("ReadPurchase() = %v", err)
	}
	if got.Status != domain.StatusConfirmed {
		t.Errorf("purchase is %s after a graceful stop, want %s — "+
			"shutdown interrupted the work instead of declining new work",
			got.Status, domain.StatusConfirmed)
	}
}

func queueDepth(t *testing.T, conn *queue.Connection, name string) int {
	t.Helper()

	depth, err := conn.QueueDepth(context.Background(), name)
	if err != nil {
		t.Fatalf("QueueDepth(%q) = %v", name, err)
	}
	return depth
}

func waitForQueueDepth(t *testing.T, conn *queue.Connection, name string, want int, within time.Duration) int {
	t.Helper()

	deadline := time.Now().Add(within)
	for {
		if depth := queueDepth(t, conn, name); depth >= want {
			return depth
		}
		if time.Now().After(deadline) {
			return queueDepth(t, conn, name)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func startPostgres(t *testing.T) *store.Store {
	t.Helper()

	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("tickets"),
		tcpostgres.WithUsername("tickets"),
		tcpostgres.WithPassword("tickets"),
		testcontainers.WithWaitStrategy(
			// Postgres starts, stops and restarts once while initialising, so
			// the readiness line appears twice. Waiting for the first
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

func startBroker(t *testing.T) *queue.Connection {
	t.Helper()

	ctx := context.Background()

	container, err := tcrabbit.Run(ctx, "rabbitmq:4-management-alpine")
	if err != nil {
		t.Fatalf("starting rabbitmq: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminating rabbitmq: %v", err)
		}
	})

	url, err := container.AmqpURL(ctx)
	if err != nil {
		t.Fatalf("reading amqp url: %v", err)
	}

	conn, err := queue.Dial(ctx, url)
	if err != nil {
		t.Fatalf("Dial() = %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("closing rabbitmq: %v", err)
		}
	})

	return conn
}
