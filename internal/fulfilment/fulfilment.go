// Package fulfilment finishes a ticket that has already been sold.
//
// The sale is decided in Redis and recorded in PostgreSQL before anything here
// runs. What is left is the slow part — drawing a document, and one day sending
// an email — which is exactly the work that must not sit on a request while
// five thousand people press Buy.
package fulfilment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/queue"
)

// Settler is the source of truth, as much of it as this package needs.
type Settler interface {
	SettlePurchase(ctx context.Context, purchaseID string, status domain.Status) (domain.Purchase, bool, error)
}

// Observer counts fulfilment attempts and times them.
//
// Queue latency is the number worth watching here, and it is not the same as
// fulfilment duration: duration says how long the work takes, latency says how
// long the ticket waited before anybody started. A backlog moves the second
// while leaving the first exactly where it was, so a dashboard with only the
// duration shows a healthy worker right up until the queue is an hour deep.
//
// Optional; a nil Observer is what the integration tests use.
type Observer interface {
	ObserveFulfilment(outcome string, elapsed time.Duration)
	ObserveQueueLatency(waited time.Duration)
}

// Fulfilment outcomes, as metric labels.
const (
	outcomeConfirmed = "confirmed"
	outcomeDuplicate = "duplicate"
	outcomePermanent = "permanent_failure"
	outcomeRetryable = "retryable_failure"
)

// Service turns a pending purchase into a confirmed one.
type Service struct {
	store    Settler
	delay    time.Duration
	timeout  time.Duration
	observer Observer
	logger   *slog.Logger
}

// WithObserver attaches metrics to a Service.
func (s *Service) WithObserver(o Observer) *Service {
	s.observer = o
	return s
}

// New builds a Service.
//
// The delay stands in for generating a PDF. The timeout bounds the write that
// follows it, which is the only real I/O here — separate budgets, because a
// slow database and a long pretend-render are different problems and a single
// number would hide which one was happening.
func New(store Settler, delay, timeout time.Duration, logger *slog.Logger) *Service {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Service{store: store, delay: delay, timeout: timeout, logger: logger}
}

// Fulfil generates the ticket and records that it exists.
//
// Idempotent, and it has to be. RabbitMQ delivers at least once, so this will
// be handed the same message twice sooner or later: a redelivery after an
// acknowledgement that never arrived, or a retry after a timeout the first
// attempt actually survived. There is no broker setting that prevents it and no
// amount of care in this function that would — the guarantee has to come from
// the write being safe to repeat, which is what SettlePurchase's WHERE clause
// provides.
func (s *Service) Fulfil(ctx context.Context, delivery queue.Delivery) error {
	message := delivery.Message
	started := time.Now()

	// Measured before any work, because this is how long the ticket waited
	// for somebody to start rather than how long they then took.
	if s.observer != nil {
		s.observer.ObserveQueueLatency(started.Sub(message.IssuedAt))
	}

	log := s.logger.With(
		slog.String("purchase_id", message.PurchaseID),
		slog.String("user_id", message.UserID),
		slog.String("correlation_id", message.CorrelationID),
		slog.Int("attempt", delivery.Attempt),
		slog.Duration("queued_for", time.Since(message.IssuedAt)))

	if err := s.generate(ctx); err != nil {
		s.observe(outcomeRetryable, started)
		return err
	}

	settleCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	purchase, changed, err := s.store.SettlePurchase(settleCtx, message.PurchaseID, domain.StatusConfirmed)

	switch {
	case errors.Is(err, domain.ErrPurchaseNotFound):
		s.observe(outcomePermanent, started)
		// The message names a purchase the source of truth has never heard
		// of. No number of retries invents one, and the two waits spent
		// trying would be two waits nobody spends looking at the real
		// problem.
		return fmt.Errorf("%w: %w", queue.ErrPermanent, err)

	case errors.Is(err, domain.ErrPurchaseNotPending):
		s.observe(outcomePermanent, started)
		// Cancelled or already failed. Something else has decided what
		// happens to this ticket, and confirming it now would overrule that
		// decision with a message that predates it.
		return fmt.Errorf("%w: %w", queue.ErrPermanent, err)

	case err != nil:
		// Everything else is worth another go: a database failing over, a
		// pool exhausted by a burst, a network that dropped one packet.
		s.observe(outcomeRetryable, started)
		return fmt.Errorf("settle purchase: %w", err)
	}

	// A redelivery of something already confirmed is a success, and counted
	// apart from a first confirmation so that a rising duplicate rate is
	// visible. A few are normal; a lot means acknowledgements are being lost.
	outcome := outcomeConfirmed
	if !changed {
		outcome = outcomeDuplicate
	}
	s.observe(outcome, started)

	log.Info("ticket fulfilled",
		slog.String("status", string(purchase.Status)),
		slog.Bool("duplicate", !changed))
	return nil
}

func (s *Service) observe(outcome string, started time.Time) {
	if s.observer != nil {
		s.observer.ObserveFulfilment(outcome, time.Since(started))
	}
}

// generate is where a PDF would be drawn.
//
// It waits on the clock and on the context together rather than calling
// time.Sleep. A sleep cannot be interrupted, so a worker holding a message
// would keep its budget and its shutdown waiting for the full delay with
// nothing to show for it — and the budget exists precisely to stop work that is
// no longer wanted.
func (s *Service) generate(ctx context.Context) error {
	if s.delay <= 0 {
		return nil
	}

	timer := time.NewTimer(s.delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("generate ticket: %w", ctx.Err())
	}
}
