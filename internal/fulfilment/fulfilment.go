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
	SettlePurchase(ctx context.Context, purchaseID string, status domain.Status) (domain.Purchase, error)
}

// Service turns a pending purchase into a confirmed one.
type Service struct {
	store   Settler
	delay   time.Duration
	timeout time.Duration
	logger  *slog.Logger
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

	log := s.logger.With(
		slog.String("purchase_id", message.PurchaseID),
		slog.String("user_id", message.UserID),
		slog.String("correlation_id", message.CorrelationID),
		slog.Int("attempt", delivery.Attempt),
		slog.Duration("queued_for", time.Since(message.IssuedAt)))

	if err := s.generate(ctx); err != nil {
		return err
	}

	settleCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	purchase, err := s.store.SettlePurchase(settleCtx, message.PurchaseID, domain.StatusConfirmed)

	switch {
	case errors.Is(err, domain.ErrPurchaseNotFound):
		// The message names a purchase the source of truth has never heard
		// of. No number of retries invents one, and the two waits spent
		// trying would be two waits nobody spends looking at the real
		// problem.
		return fmt.Errorf("%w: %w", queue.ErrPermanent, err)

	case errors.Is(err, domain.ErrPurchaseNotPending):
		// Cancelled or already failed. Something else has decided what
		// happens to this ticket, and confirming it now would overrule that
		// decision with a message that predates it.
		return fmt.Errorf("%w: %w", queue.ErrPermanent, err)

	case err != nil:
		// Everything else is worth another go: a database failing over, a
		// pool exhausted by a burst, a network that dropped one packet.
		return fmt.Errorf("settle purchase: %w", err)
	}

	log.Info("ticket fulfilled", slog.String("status", string(purchase.Status)))
	return nil
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
