//go:build integration

// Integration tests for the RabbitMQ layer, against a real broker started by
// Testcontainers.
//
// Nothing here is mocked, for the same reason nothing in the cache tests is. A
// fake broker confirms every publish, redelivers on command and never returns a
// message as unroutable — which means it agrees with whatever this code already
// believes, and the beliefs are the thing being tested.
package queue

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

// startBroker runs one throwaway RabbitMQ and returns a connection with the
// topology already declared.
func startBroker(t *testing.T) *Connection {
	conn, _ := startBrokerContainer(t)
	return conn
}

// startBrokerContainer is startBroker plus the container handle, for the one
// test that has to take the broker away mid-flight.
func startBrokerContainer(t *testing.T) (*Connection, testcontainers.Container) {
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

	conn, err := Dial(ctx, url)
	if err != nil {
		t.Fatalf("Dial() = %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("closing rabbitmq: %v", err)
		}
	})

	return conn, container
}

func newTestPublisher(t *testing.T, conn *Connection) *Publisher {
	t.Helper()

	publisher, err := NewPublisher(conn, 4)
	if err != nil {
		t.Fatalf("NewPublisher() = %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Logf("closing publisher: %v", err)
		}
	})

	return publisher
}

func testMessage(purchaseID string) TicketMessage {
	return TicketMessage{
		PurchaseID:     purchaseID,
		CampaignID:     "queima-test",
		UserID:         "student-1",
		IdempotencyKey: "6f9619ff-8b86-d011-b42d-00cf4fc964ff",
		CorrelationID:  "correlation-1",
		IssuedAt:       time.Now().UTC(),
	}
}

// consumeInto runs a consumer in the background and hands every delivery to fn.
// It returns a function that stops the consumer and waits for it.
func consumeInto(t *testing.T, conn *Connection, publisher *Publisher, fn Handler) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewConsumer(conn, publisher, 1, 30*time.Second, slog.New(slog.DiscardHandler))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := consumer.Consume(ctx, fn); err != nil && ctx.Err() == nil {
			t.Errorf("Consume() = %v", err)
		}
	}()

	stopped := func() {
		cancel()
		wg.Wait()
	}
	t.Cleanup(stopped)
	return stopped
}

// The basic promise: a confirmed publish reaches a consumer intact.
func TestAPublishedTicketReachesTheWorker(t *testing.T) {
	conn := startBroker(t)
	publisher := newTestPublisher(t, conn)

	received := make(chan Delivery, 1)
	consumeInto(t, conn, publisher, func(_ context.Context, d Delivery) error {
		received <- d
		return nil
	})

	sent := testMessage("purchase-1")
	if err := publisher.PublishTicket(context.Background(), sent); err != nil {
		t.Fatalf("PublishTicket() = %v", err)
	}

	select {
	case got := <-received:
		if got.Message.PurchaseID != sent.PurchaseID {
			t.Errorf("purchase id = %q, want %q", got.Message.PurchaseID, sent.PurchaseID)
		}
		if got.Message.CorrelationID != sent.CorrelationID {
			t.Errorf("correlation id = %q, want %q", got.Message.CorrelationID, sent.CorrelationID)
		}
		if got.Attempt != 1 {
			t.Errorf("attempt = %d on first delivery, want 1", got.Attempt)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the message never arrived")
	}
}

// A publish nobody is listening for must not report success.
//
// This is the case publisher confirms alone do not cover, and the reason the
// publisher tracks returns as well: the broker acknowledges an unroutable
// message perfectly happily, having thrown it away. Without the return, a
// ticket vanishes and the API tells the client it is on its way.
func TestAnUnroutablePublishIsReportedRatherThanSilentlyDropped(t *testing.T) {
	conn := startBroker(t)
	publisher := newTestPublisher(t, conn)

	err := publisher.publish(context.Background(), "ticket.nowhere", testMessage("purchase-1"), 1)
	if !errors.Is(err, ErrUnroutable) {
		t.Errorf("publish to an unbound key = %v, want %v", err, ErrUnroutable)
	}
}

// A failing handler gets the message back, after a wait, with the attempt
// counter moved on.
//
// The wait is the part worth testing. A straight requeue comes round again
// immediately, so a dependency that is down is hammered until the attempts run
// out — which turns three retries into three failures inside a millisecond and
// makes the retry budget meaningless.
func TestAFailedMessageComesBackLaterAsTheNextAttempt(t *testing.T) {
	conn := startBroker(t)
	publisher := newTestPublisher(t, conn)

	attempts := make(chan Delivery, 4)
	consumeInto(t, conn, publisher, func(_ context.Context, d Delivery) error {
		attempts <- d
		return errors.New("fulfilment failed")
	})

	published := time.Now()
	if err := publisher.PublishTicket(context.Background(), testMessage("purchase-1")); err != nil {
		t.Fatalf("PublishTicket() = %v", err)
	}

	first := waitForDelivery(t, attempts, 20*time.Second)
	if first.Attempt != 1 {
		t.Errorf("first delivery was attempt %d, want 1", first.Attempt)
	}

	second := waitForDelivery(t, attempts, 30*time.Second)
	if second.Attempt != 2 {
		t.Errorf("second delivery was attempt %d, want 2", second.Attempt)
	}

	// The first retry tier waits five seconds. Allowing a generous margin
	// below it still catches the failure this is guarding against, which is a
	// redelivery with no delay at all.
	if elapsed := time.Since(published); elapsed < 4*time.Second {
		t.Errorf("the retry came back after %s, want at least the tier's delay", elapsed)
	}
}

// A message that fails every attempt stops, and stops somewhere visible.
//
// The alternative — retrying forever — is the failure mode where a single
// poisonous message occupies a worker indefinitely and nothing in the system
// says so.
func TestAMessageThatFailsEveryAttemptEndsInTheDeadLetterQueue(t *testing.T) {
	conn := startBroker(t)
	publisher := newTestPublisher(t, conn)

	var mu sync.Mutex
	var seen int

	consumeInto(t, conn, publisher, func(_ context.Context, _ Delivery) error {
		mu.Lock()
		seen++
		mu.Unlock()
		return errors.New("fulfilment failed")
	})

	if err := publisher.PublishTicket(context.Background(), testMessage("purchase-1")); err != nil {
		t.Fatalf("PublishTicket() = %v", err)
	}

	// Five seconds for the first retry, thirty for the second, plus room for
	// a slow broker.
	deadLettered := waitForQueueDepth(t, conn, DeadLetterQueue, 1, 90*time.Second)
	if deadLettered != 1 {
		t.Fatalf("the dead letter queue holds %d messages, want 1", deadLettered)
	}

	mu.Lock()
	defer mu.Unlock()
	if seen != MaxAttempts {
		t.Errorf("the handler ran %d times, want %d", seen, MaxAttempts)
	}

	// And nothing is left going round.
	if depth := queueDepth(t, conn, ProcessingQueue); depth != 0 {
		t.Errorf("the processing queue still holds %d messages, want 0", depth)
	}
}

// A handler that succeeds is acknowledged, so the message does not come round
// again when the consumer restarts.
func TestASucceededMessageIsNotRedelivered(t *testing.T) {
	conn := startBroker(t)
	publisher := newTestPublisher(t, conn)

	delivered := make(chan Delivery, 4)
	stop := consumeInto(t, conn, publisher, func(_ context.Context, d Delivery) error {
		delivered <- d
		return nil
	})

	if err := publisher.PublishTicket(context.Background(), testMessage("purchase-1")); err != nil {
		t.Fatalf("PublishTicket() = %v", err)
	}
	waitForDelivery(t, delivered, 20*time.Second)

	stop()

	if depth := queueDepth(t, conn, ProcessingQueue); depth != 0 {
		t.Errorf("the processing queue holds %d messages after a successful ack, want 0", depth)
	}
}

func waitForDelivery(t *testing.T, deliveries <-chan Delivery, within time.Duration) Delivery {
	t.Helper()

	select {
	case d := <-deliveries:
		return d
	case <-time.After(within):
		t.Fatalf("no delivery within %s", within)
		return Delivery{}
	}
}

func queueDepth(t *testing.T, conn *Connection, name string) int {
	t.Helper()

	depth, err := conn.QueueDepth(context.Background(), name)
	if err != nil {
		t.Fatalf("QueueDepth(%q) = %v", name, err)
	}
	return depth
}

func waitForQueueDepth(t *testing.T, conn *Connection, name string, want int, within time.Duration) int {
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

// A publish must answer the caller even when the broker has stopped answering.
//
// The library makes this easy to get wrong.
// PublishWithDeferredConfirmWithContext reads as though it honours a deadline,
// and does not: it checks the context once before doing anything and then calls
// the fully synchronous publish, which writes the frame under a
// connection-wide mutex with no deadline of any kind. A broker that stops
// reading without closing the socket therefore blocks the write, and with it
// every other channel on that connection.
//
// What that costs sits well above this package. The sale has already taken a
// ticket out of Redis and written a pending row by the time the publish runs,
// so a publish that never returns is a request that never returns: the caller
// is told nothing, the goroutine and its ticket are stranded, and neither
// PUBLISH_TIMEOUT nor REQUEST_TIMEOUT can do anything about it — they cancel a
// context nothing is reading any more.
//
// Note what this test does and does not prove. Stopping the container produces
// a hanging write on some platforms and a clean connection reset on others, so
// on a runner where the socket is reset this passes without exercising the
// abandonment at all. The property asserted is the one that has to hold either
// way: the call returns inside the caller's budget, whatever the broker does.
func TestAPublishReturnsWhenTheBrokerStopsAnswering(t *testing.T) {
	conn, container := startBrokerContainer(t)
	publisher := newTestPublisher(t, conn)

	// A sale first, so the test is measuring a publisher that was working.
	if err := publisher.PublishTicket(context.Background(), testMessage("purchase-before")); err != nil {
		t.Fatalf("PublishTicket() before the outage = %v", err)
	}

	timeout := 10 * time.Second
	if err := container.Stop(context.Background(), &timeout); err != nil {
		t.Fatalf("stopping the broker: %v", err)
	}

	const budget = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	done := make(chan error, 1)
	started := time.Now()
	go func() { done <- publisher.PublishTicket(ctx, testMessage("purchase-during")) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("PublishTicket() = nil with the broker stopped, want a failure")
		}
		// Never reported as refused or unroutable. Those are definitive, and
		// acting on them reverses the sale — which would be exactly wrong
		// here, because the broker may have taken the message before it went.
		for _, definitive := range []error{ErrPublishRefused, ErrUnroutable} {
			if errors.Is(err, definitive) {
				t.Errorf("reported %v, which tells the caller the message is certainly gone; it is not", definitive)
			}
		}
		t.Logf("returned after %s: %v", time.Since(started).Round(time.Millisecond), err)

	case <-time.After(budget + 8*time.Second):
		t.Fatal("PublishTicket() never returned: the caller's budget cannot end a publish, " +
			"so the request that made this sale would hang until the client gave up")
	}
}
