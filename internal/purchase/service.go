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
	"fmt"
	"log/slog"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/cache"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/store"
)

// Service sells tickets.
type Service struct {
	cache  *cache.Cache
	store  *store.Store
	logger *slog.Logger
}

// New builds a Service.
func New(c *cache.Cache, s *store.Store, logger *slog.Logger) *Service {
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
// separate systems. This is the gap the whole rest of the project is about. It
// is narrowed here, not closed — if recording the purchase fails, the ticket is
// handed back; if the *process dies* between the two, the ticket is gone until
// something notices. Phase 5 is where something notices.
func (s *Service) Purchase(ctx context.Context, campaignID, userID string) (domain.Purchase, error) {
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

	purchase, err := s.store.RecordPurchase(ctx, campaignID, userID)
	if err != nil {
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
