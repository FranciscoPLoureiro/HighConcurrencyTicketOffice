// Package compensation undoes a sale that could not be fulfilled.
//
// It drains the dead letter queue, which is a change of role for that queue
// worth being explicit about: until phase 5 its purpose was to stop and be
// looked at, and draining one automatically is normally how a poisonous message
// gets retried forever. It is safe here only because this does not retry
// anything. The message is already known not to work; what is worth rescuing is
// the ticket attached to it.
//
// The saga is three writes in a deliberate order, and every one of them is safe
// to repeat, because the thing running them is fed by a broker that delivers at
// least once.
package compensation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/queue"
)

// Recorder is the source of truth, as much of it as this package needs.
type Recorder interface {
	CancelPurchase(ctx context.Context, purchaseID string) (domain.Purchase, error)
	ReadPurchase(ctx context.Context, campaignID, purchaseID string) (domain.Purchase, error)
}

// Releaser puts a ticket back on the shelf.
type Releaser interface {
	Release(ctx context.Context, campaignID, userID string) (bool, error)
}

// Notifier records that a sale was reversed, for whatever wants to know.
type Notifier interface {
	Compensate(ctx context.Context, message queue.TicketMessage) error
}

// Observer counts reversals.
type Observer interface {
	Compensated(outcome string)
}

// Service reverses sales that failed.
type Service struct {
	store    Recorder
	cache    Releaser
	notifier Notifier
	timeout  time.Duration
	observer Observer
	logger   *slog.Logger
}

// New builds a Service.
func New(store Recorder, cache Releaser, notifier Notifier, timeout time.Duration, logger *slog.Logger) *Service {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Service{store: store, cache: cache, notifier: notifier, timeout: timeout, logger: logger}
}

// WithObserver attaches metrics.
func (s *Service) WithObserver(o Observer) *Service {
	s.observer = o
	return s
}

// Compensate reverses one failed sale.
//
// The order is PostgreSQL, then Redis, then the notification, and it is the
// same order the purchase path reverses in for the same reason. If the second
// step fails, Redis is one ticket short of the truth: the campaign undersells
// by one and the next reconciliation puts it right. The other order fails the
// other way — the ticket back on the shelf while a live row still says whose it
// is — which is an oversell plus a person the fairness index will not let back
// in.
//
// Every step is idempotent, and that is not a nicety here. The broker delivers
// at least once, so this will be handed the same message twice sooner or later,
// and the brief is explicit about what happens if it is careless: "if the
// compensation runs twice, the stock increments twice and you now have 101
// tickets". CancelPurchase moves a row only if it is not already cancelled;
// release.lua tests and increments in one step. Neither can run twice.
func (s *Service) Compensate(ctx context.Context, delivery queue.Delivery) error {
	message := delivery.Message

	log := s.logger.With(
		slog.String("purchase_id", message.PurchaseID),
		slog.String("user_id", message.UserID),
		slog.String("correlation_id", message.CorrelationID))

	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	// A purchase that never existed cannot be reversed, and retrying will not
	// make one appear. It is dropped rather than returned, or it comes back
	// forever.
	purchase, err := s.store.CancelPurchase(ctx, message.PurchaseID)
	switch {
	case errors.Is(err, domain.ErrPurchaseNotFound):
		log.Warn("nothing to compensate: no such purchase")
		return nil
	case err != nil:
		return fmt.Errorf("cancel purchase: %w", err)
	}

	// Deliberately not conditional on the cancellation having changed
	// anything. A previous attempt may have cancelled the row and then failed
	// before returning the ticket, and skipping this because the row was
	// "already done" would strand the ticket permanently — the one state
	// nothing else in the system will ever revisit.
	released, err := s.cache.Release(ctx, purchase.CampaignID, purchase.UserID)
	if err != nil {
		return fmt.Errorf("return the ticket to the shelf: %w", err)
	}

	log.Warn("reversed a sale that could not be fulfilled",
		slog.Bool("ticket_returned", released),
		slog.String("status", string(purchase.Status)))

	if s.observer != nil {
		s.observer.Compensated("reversed")
	}

	// Last, and its failure is not the saga's failure. The ticket is back and
	// the row is cancelled, which is the part that matters; a lost
	// notification is worth a line and is not worth undoing a correct
	// compensation over — nor worth having the message redelivered to do the
	// whole thing again.
	if err := s.notifier.Compensate(ctx, message); err != nil {
		log.Error("compensated, but could not publish the notification",
			slog.Any("error", err))
	}

	return nil
}
