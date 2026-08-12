package purchase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/cache"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/queue"
)

// The sweeper closes the gap phase 5 exists for.
//
// A sale is three writes to three systems — Redis decides, PostgreSQL records,
// the broker takes the work away — and a process that dies partway through
// leaves the ticket in one of two states, neither of which resolves on its own:
//
//	killed after the decrement, before the row
//	    Redis says sold. PostgreSQL has never heard of it. The ticket belongs
//	    to nobody and cannot be bought by anyone, and with a hundred tickets a
//	    handful of well-placed crashes closes the campaign having sold nothing.
//
//	killed after the row, before the publish
//	    The row is pending and no worker will ever see it. The seat is taken
//	    and the person holding it waits for a document that is not coming.
//
// The reservation taken by the Lua script is what makes the first case visible
// later, and the pending row makes the second visible. This sweeps both, and
// the direction it errs in is the same as everywhere else in this system: it
// gives a ticket back only when the source of truth says nobody has it.

// SweepResult is what one pass found and did.
type SweepResult struct {
	// Examined is how many expired reservations were looked at.
	Examined int
	// Released is how many tickets went back on the shelf.
	Released int
	// Closed is how many reservations belonged to a real purchase and were
	// simply closed. Not a failure: it is what a crash between the publish
	// and the confirmation leaves behind.
	Closed int
	// Republished is how many pending purchases were sent for fulfilment
	// again because nothing appeared to be fulfilling them.
	Republished int
}

// SweepPolicy is how long a thing has to have been stuck to count as abandoned.
type SweepPolicy struct {
	// ReservationAge is how long a reservation may stay open before the
	// sweeper will consider releasing it.
	//
	// This is the safety-critical number. It has to exceed the longest a
	// legitimate request can take, by a margin, because releasing a
	// reservation whose sale is still in flight sells the same seat twice —
	// the one outcome this project exists to prevent. The request timeout
	// bounds a purchase at ten seconds, so a reservation still open after a
	// minute belongs to a request that is not coming back.
	ReservationAge time.Duration

	// PendingAge is how long a purchase may sit pending before the sweeper
	// republishes it. Longer than fulfilment takes, or the sweeper competes
	// with the worker it is meant to be backing up.
	PendingAge time.Duration

	// Batch bounds one pass, so that a sweeper starting after a long outage
	// does not turn a recovery into a database stampede of its own.
	Batch int64
}

// DefaultSweepPolicy is what the service runs with when a caller supplies none.
var DefaultSweepPolicy = SweepPolicy{
	ReservationAge: time.Minute,
	PendingAge:     2 * time.Minute,
	Batch:          500,
}

// Sweep runs one pass and reports what it did.
//
// Under the reconciliation lock, so that several instances do not each decide
// the same reservation is abandoned. That is belt and braces rather than
// load-bearing — releasing is idempotent, and two sweepers agreeing would
// return one ticket between them — but the database reads are not free, and
// there is no reason to pay for them once per instance.
func (s *Service) Sweep(ctx context.Context, campaignID string) (SweepResult, error) {
	var result SweepResult

	err := s.cache.WithLock(ctx, cache.ReconcileLockKey(campaignID), sweepLockTTL, sweepLockWait,
		func(ctx context.Context) error {
			var err error
			result, err = s.sweepLocked(ctx, campaignID)
			return err
		})
	if err != nil {
		if errors.Is(err, cache.ErrLockUnavailable) {
			// Another instance is sweeping, or starting up and reconciling.
			// Both are fine and neither is worth an error: this pass has
			// nothing to add, and the next one is a tick away.
			return SweepResult{}, nil
		}
		return SweepResult{}, fmt.Errorf("sweep campaign %q: %w", campaignID, err)
	}

	return result, nil
}

const (
	// Short, because a sweep is a handful of reads and at most a few hundred
	// small writes, and a holder that dies must not block the next pass for
	// long.
	sweepLockTTL = 30 * time.Second
	// Zero wait. A sweeper that cannot have the lock has nothing useful to do
	// with it — queueing behind a peer only to redo work that peer has just
	// done is a waste with a timeout attached.
	sweepLockWait = 0
)

func (s *Service) sweepLocked(ctx context.Context, campaignID string) (SweepResult, error) {
	var result SweepResult

	expired, err := s.cache.ExpiredReservations(ctx, campaignID, s.sweep.ReservationAge, s.sweep.Batch)
	if err != nil {
		return result, fmt.Errorf("read expired reservations: %w", err)
	}
	result.Examined = len(expired)

	for _, reservation := range expired {
		released, err := s.settleReservation(ctx, campaignID, reservation)
		if err != nil {
			// One stuck reservation must not stop the rest of the pass. The
			// entry stays where it is and the next pass tries again.
			s.logger.Error("could not settle an expired reservation",
				slog.String("campaign_id", campaignID),
				slog.String("user_id", reservation.UserID),
				slog.Any("error", err))
			continue
		}
		if released {
			result.Released++
		} else {
			result.Closed++
		}
	}

	result.Republished, err = s.republishStalled(ctx, campaignID)
	if err != nil {
		return result, err
	}

	if result.Released > 0 || result.Republished > 0 {
		s.logger.Warn("sweeper recovered tickets that were stuck",
			slog.String("campaign_id", campaignID),
			slog.Int("examined", result.Examined),
			slog.Int("released", result.Released),
			slog.Int("closed", result.Closed),
			slog.Int("republished", result.Republished))
	}

	return result, nil
}

// settleReservation decides what one abandoned reservation deserves, and
// reports whether a ticket went back on the shelf.
//
// The database is asked first, every time, and that ordering is the whole
// safety argument. A reservation is only evidence that a ticket *left* the
// shelf; whether the sale completed is a fact only the source of truth holds.
// Releasing without asking would hand back seats that people are holding
// perfectly valid rows for.
func (s *Service) settleReservation(ctx context.Context, campaignID string, reservation cache.Reservation) (bool, error) {
	held, err := s.store.HasLiveTicket(ctx, campaignID, reservation.UserID)
	if err != nil {
		return false, fmt.Errorf("check for a live ticket: %w", err)
	}

	if held {
		// The sale went through and only the confirmation was lost — a crash
		// between the publish and closing the reservation. Nothing is owed;
		// the entry is just stale.
		if _, err := s.cache.Confirm(ctx, campaignID, reservation.UserID); err != nil {
			return false, fmt.Errorf("close a completed reservation: %w", err)
		}
		return false, nil
	}

	// PostgreSQL has never heard of this sale, and the request that would have
	// told it is long dead. The ticket goes back.
	released, err := s.cache.Release(ctx, campaignID, reservation.UserID)
	if err != nil {
		return false, fmt.Errorf("release an abandoned reservation: %w", err)
	}

	s.logger.Warn("returned a ticket whose sale never completed",
		slog.String("campaign_id", campaignID),
		slog.String("user_id", reservation.UserID),
		slog.Duration("open_for", reservation.Age(time.Now())))

	if s.observer != nil {
		s.observer.Compensated("swept")
	}

	return released, nil
}

// republishStalled sends pending purchases back for fulfilment.
//
// This is the second crash window: the row committed and the publish did not,
// so the seat is taken and nothing anywhere is going to produce a ticket for
// it. Nobody finds out by waiting, because a pending purchase looks exactly
// like one whose worker is merely busy.
//
// Republishing is safe precisely because the worker is idempotent — settling a
// purchase moves a row only if it is still pending, so a duplicate finds
// nothing to do. That property was built for the broker's at-least-once
// delivery and this reuses it rather than inventing a second mechanism.
func (s *Service) republishStalled(ctx context.Context, campaignID string) (int, error) {
	stalled, err := s.store.StalledPurchases(ctx, campaignID, s.sweep.PendingAge, int(s.sweep.Batch))
	if err != nil {
		return 0, fmt.Errorf("read stalled purchases: %w", err)
	}

	var republished int
	for _, purchase := range stalled {
		if err := s.fulfiller.PublishTicket(ctx, queue.TicketMessage{
			PurchaseID:     purchase.ID,
			CampaignID:     purchase.CampaignID,
			UserID:         purchase.UserID,
			IdempotencyKey: purchase.IdempotencyKey,
			CorrelationID:  sweepCorrelationID(purchase.ID),
			IssuedAt:       time.Now().UTC(),
		}); err != nil {
			s.logger.Error("could not republish a stalled purchase",
				slog.String("purchase_id", purchase.ID),
				slog.Any("error", err))
			continue
		}

		s.logger.Warn("republished a purchase nothing was fulfilling",
			slog.String("campaign_id", campaignID),
			slog.String("purchase_id", purchase.ID),
			slog.String("user_id", purchase.UserID))

		// The reservation, if one is still open, is now answered.
		if _, err := s.cache.Confirm(ctx, campaignID, purchase.UserID); err != nil {
			s.logger.Warn("republished but could not close the reservation",
				slog.String("purchase_id", purchase.ID),
				slog.Any("error", err))
		}

		republished++
	}

	return republished, nil
}

// sweepCorrelationID marks a message the sweeper sent rather than a client.
//
// The original request's identifier died with the process that was serving it,
// and inventing a fresh opaque one would leave a worker's log line joining to
// nothing. This at least says where the message came from and which purchase it
// is about, which is the question anybody reading it will have.
func sweepCorrelationID(purchaseID string) string { return "sweeper:" + purchaseID }

// Reserved reports how many sales are in flight or stuck, for a test to assert
// and a dashboard to draw.
func (s *Service) Reserved(ctx context.Context, campaignID string) (int64, error) {
	return s.cache.CountReservations(ctx, campaignID)
}
