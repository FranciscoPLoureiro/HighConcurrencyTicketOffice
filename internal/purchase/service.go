// Package purchase joins the two systems that a sale touches: Redis, which
// decides, and PostgreSQL, which remembers.
//
// It exists as its own package because that join is the interesting part of the
// design and does not belong to either side of it. The cache package knows
// nothing about purchases as records; the store package knows nothing about
// stock being enforced somewhere else.
package purchase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/cache"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/store"
)

// Decider is the half of the system that decides, and Recorder is the half that
// remembers. *cache.Cache and *store.Store satisfy them, and nothing else in
// the production build does.
//
// They exist because the interesting behaviour in this package is what happens
// when one of the two halves fails, and a test that cannot make Redis or
// PostgreSQL fail on demand cannot reach any of it. The happy path is covered
// against real containers, where it belongs; the compensation policy is covered
// here, where a failure is one line of test code instead of a killed container
// and a race to hit the right instant.
type Decider interface {
	Purchase(ctx context.Context, campaignID, userID string) (cache.Outcome, int64, error)
	Release(ctx context.Context, campaignID, userID string) (bool, error)
	Remaining(ctx context.Context, campaignID string) (int64, bool, error)
	Reconcile(ctx context.Context, campaignID string, remaining int, buyers []string) error
	WithLock(ctx context.Context, key string, ttl, wait time.Duration, fn func(context.Context) error) error
}

// Recorder is the source of truth.
type Recorder interface {
	RecordPending(ctx context.Context, campaignID, userID, idempotencyKey string) (domain.Purchase, error)
	ReadCampaignState(ctx context.Context, campaignID string) (store.CampaignState, error)
}

// Service sells tickets.
type Service struct {
	cache  Decider
	store  Recorder
	logger *slog.Logger
}

// New builds a Service.
func New(c Decider, s Recorder, logger *slog.Logger) *Service {
	return &Service{cache: c, store: s, logger: logger}
}

// compensationBudget bounds the attempt to put a ticket back after the write to
// PostgreSQL failed. It is short because the request is already lost and the
// caller is already waiting on an error.
const compensationBudget = 5 * time.Second

// Purchase sells one ticket to one user.
//
// Redis decides and PostgreSQL records. The decision is a single atomic script,
// so the two invariants — at most one hundred tickets, at most one per person —
// hold no matter how many requests arrive together.
//
// The two writes are not atomic with each other, and cannot be: they are
// separate systems. This is the gap the whole rest of the project is about, and
// it is narrowed here rather than closed. If recording the purchase is known to
// have failed, the ticket is handed back. If it cannot be known — the process
// dies between the two, or the reply to either write is simply lost — the
// ticket stays out of circulation until something notices. Phase 5 is where
// something notices.
func (s *Service) Purchase(ctx context.Context, campaignID, userID, idempotencyKey string) (domain.Purchase, error) {
	outcome, remaining, err := s.cache.Purchase(ctx, campaignID, userID)
	if err != nil {
		// Fail closed. Redis is the only place the stock invariant is
		// enforced, so a purchase that cannot consult it is a purchase
		// nobody can prove is safe. Selling anyway would trade a failed
		// request for an oversold campaign.
		return domain.Purchase{}, fmt.Errorf("decide purchase: %w", err)
	}

	switch outcome {
	case cache.AlreadyHeld:
		return domain.Purchase{}, domain.ErrAlreadyPurchased
	case cache.SoldOut:
		return domain.Purchase{}, domain.ErrSoldOut
	case cache.Uninitialised:
		// Not a refusal — a system that has not been told what it is
		// selling. Startup reconciliation is supposed to make this
		// unreachable, so it is worth a loud line rather than a quiet 404.
		s.logger.Error("purchase attempted against a campaign with no stock in redis",
			slog.String("campaign_id", campaignID))
		return domain.Purchase{}, domain.ErrCampaignNotFound
	case cache.Sold:
	}

	purchase, err := s.store.RecordPending(ctx, campaignID, userID, idempotencyKey)
	if err != nil {
		// Hand the ticket back only when the write is known not to have
		// happened. A failure at COMMIT does not say that, and treating it as
		// though it did is how a hundred tickets becomes a hundred and one:
		// if the transaction committed after all, the compensation returns
		// stock that PostgreSQL has already given away and un-marks a buyer
		// who really does hold a ticket.
		//
		// Doing nothing costs at most one ticket that nobody can buy until
		// the next reconciliation. That is the direction this system errs in
		// everywhere else, and the only one of the two that is recoverable.
		if errors.Is(err, store.ErrOutcomeUnknown) {
			s.logger.Error("purchase may or may not have been recorded, leaving the ticket out of circulation",
				slog.String("campaign_id", campaignID),
				slog.String("user_id", userID),
				slog.Any("error", err))
			return domain.Purchase{}, fmt.Errorf("record purchase: %w", err)
		}

		s.compensate(ctx, campaignID, userID, err)
		return domain.Purchase{}, fmt.Errorf("record purchase: %w", err)
	}

	s.logger.Debug("ticket sold",
		slog.String("campaign_id", campaignID),
		slog.String("user_id", userID),
		slog.String("purchase_id", purchase.ID),
		slog.Int64("remaining", remaining))

	return purchase, nil
}

// compensate returns a ticket that Redis granted and PostgreSQL refused.
//
// Without this the ticket is decremented from a stock it will never leave: it
// belongs to nobody, cannot be sold, and the campaign quietly ends one ticket
// short for every failed write.
func (s *Service) compensate(ctx context.Context, campaignID, userID string, cause error) {
	// A detached context. The usual reason RecordPurchase failed is that the
	// caller's context was cancelled or timed out, and reusing it here would
	// mean the compensation fails for the same reason, every time — leaking
	// a ticket in exactly the case the compensation was written for.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), compensationBudget)
	defer cancel()

	released, err := s.cache.Release(ctx, campaignID, userID)
	if err != nil {
		// Both writes have now failed. The ticket stays out of circulation
		// until reconciliation next runs, which is the safe direction — the
		// campaign undersells by one rather than overselling by one.
		s.logger.Error("could not return a ticket after the purchase failed to record",
			slog.String("campaign_id", campaignID),
			slog.String("user_id", userID),
			slog.Any("cause", cause),
			slog.Any("error", err))
		return
	}

	s.logger.Warn("returned a ticket after the purchase failed to record",
		slog.String("campaign_id", campaignID),
		slog.String("user_id", userID),
		slog.Bool("released", released),
		slog.Any("cause", cause))
}
