package purchase

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/cache"
)

// Reconciliation timings.
//
// The lock TTL is the answer to "how long may an instance that died mid-way
// block everyone else?" and the wait is the answer to "how long will an
// instance queue behind a peer that is still working?". Both are generous
// relative to the work — reading a few hundred rows and writing two keys — and
// deliberately so: the cost of being wrong here is a slow start, and the cost
// of being too eager is two instances rebuilding at once.
const (
	reconcileLockTTL  = 30 * time.Second
	reconcileLockWait = 60 * time.Second
)

// Reconciliation is what a startup reconciliation found and did.
type Reconciliation struct {
	// Remaining is the stock written to Redis.
	Remaining int
	// Holders is how many people were restored to the buyer set.
	Holders int
	// PreviousRemaining is what Redis believed beforehand, and Existed says
	// whether it believed anything at all. Together they are the difference
	// between a cold start and a restart that would have refilled the shelf.
	PreviousRemaining int64
	Existed           bool
}

// Reconcile rebuilds a campaign's Redis state from PostgreSQL.
//
// This runs at startup, before the process serves anything, and it is the
// difference between a system that survives a restart and one that resells
// tickets it has already sold. The naive alternative — writing the campaign
// size into Redis on boot — turns every deploy, crash and OOM kill into a
// refill of stock that is already in people's hands.
//
// PostgreSQL is the source of truth and Redis is a projection of it. That
// sentence is only true if something actually rebuilds the projection, and this
// is that something.
//
// The whole read-then-write runs under a distributed lock so that several
// instances starting together — which a rolling deploy guarantees — do not
// interleave. The PostgreSQL read happens inside the lock rather than before
// it, which keeps the window between "what the database said" and "what Redis
// now believes" as small as it can be made.
func (s *Service) Reconcile(ctx context.Context, campaignID string) (Reconciliation, error) {
	var result Reconciliation

	err := s.cache.WithLock(ctx, cache.ReconcileLockKey(campaignID), reconcileLockTTL, reconcileLockWait,
		func(ctx context.Context) error {
			previous, existed, err := s.cache.Remaining(ctx, campaignID)
			if err != nil {
				return fmt.Errorf("read current stock: %w", err)
			}

			state, err := s.store.ReadCampaignState(ctx, campaignID)
			if err != nil {
				return fmt.Errorf("read campaign state: %w", err)
			}

			if !state.Consistent() {
				// The counter column and the purchase rows disagree, so
				// one of them is wrong and this reconciliation is about to
				// believe the rows. Worth saying out loud: the two are
				// written in one transaction on the phase 2 path, so a
				// divergence means either a phase 1 leftover or a bug.
				s.logger.Warn("postgres counter disagrees with the purchase rows",
					slog.String("campaign_id", campaignID),
					slog.Int("available_column", state.Available),
					slog.Int("total_minus_live_tickets", state.Total-len(state.Buyers)))
			}

			if state.Oversold() {
				// Arithmetic says the remaining stock is negative. That is
				// not a number Redis can usefully hold — it would count
				// upwards from below zero as tickets were cancelled and
				// refuse everyone in the meantime — so Remaining clamps it
				// and this says so out loud. A database in this state is
				// evidence of a real failure, and the phase 1 baseline is
				// one way to produce it.
				s.logger.Error("database holds more live tickets than the campaign has",
					slog.String("campaign_id", campaignID),
					slog.Int("total", state.Total),
					slog.Int("live_tickets", len(state.Buyers)))
			}

			result = Reconciliation{
				Remaining:         state.Remaining(),
				Holders:           len(state.Buyers),
				PreviousRemaining: previous,
				Existed:           existed,
			}

			return s.cache.Reconcile(ctx, campaignID, state.Remaining(), state.Buyers)
		})
	if err != nil {
		return Reconciliation{}, fmt.Errorf("reconcile campaign %q: %w", campaignID, err)
	}

	s.logger.Info("reconciled redis from postgres",
		slog.String("campaign_id", campaignID),
		slog.Int("remaining", result.Remaining),
		slog.Int("holders", result.Holders),
		slog.Bool("redis_had_state", result.Existed),
		slog.Int64("previous_remaining", result.PreviousRemaining))

	return result, nil
}
