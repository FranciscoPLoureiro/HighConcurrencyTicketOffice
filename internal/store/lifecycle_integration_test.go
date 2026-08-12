//go:build integration

package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/google/uuid"
)

// openCampaign creates a campaign and returns a helper that sells into it.
func openCampaign(t *testing.T, s *Store, total int) {
	t.Helper()

	if err := s.EnsureCampaign(context.Background(), testCampaign, total); err != nil {
		t.Fatalf("EnsureCampaign() = %v", err)
	}
}

func sell(t *testing.T, s *Store, user string) domain.Purchase {
	t.Helper()

	purchase, err := s.RecordPending(context.Background(), testCampaign, user, uuid.NewString())
	if err != nil {
		t.Fatalf("RecordPending() = %v", err)
	}
	return purchase
}

// The bug the brief says an interviewer will look for, in its PostgreSQL half:
// "if the compensation runs twice, the stock increments twice and you now have
// 101 tickets".
//
// It cannot happen here because the test and the increment are one statement.
// The UPDATE only matches a row that is not already cancelled, so the second
// call matches nothing and the counter is never touched. Written as a read
// followed by a write, both calls would read "pending" and both would increment.
func TestCancellingTwiceReturnsOneTicket(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	openCampaign(t, s, 10)

	purchase := sell(t, s, "student-1")
	if available := availability(t, s, testCampaign); available != 9 {
		t.Fatalf("available = %d after one sale, want 9", available)
	}

	for range 2 {
		if _, err := s.CancelPurchase(ctx, purchase.ID); err != nil {
			t.Fatalf("CancelPurchase() = %v", err)
		}
	}

	if available := availability(t, s, testCampaign); available != 10 {
		t.Errorf("available = %d after cancelling one ticket twice, want 10", available)
	}

	live, err := s.CountLiveTickets(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountLiveTickets() = %v", err)
	}
	if live != 0 {
		t.Errorf("%d live tickets after the cancellation, want 0", live)
	}
}

// The same guarantee under a race rather than in sequence, which is how it
// would actually arrive: a compensation and a sweeper reaching for one purchase
// at the same moment.
func TestConcurrentCancellationsReturnOneTicket(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	openCampaign(t, s, 10)

	purchase := sell(t, s, "student-1")

	const attempts = 20
	start := make(chan struct{})
	var wg sync.WaitGroup
	var failures atomic.Int64

	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			if _, err := s.CancelPurchase(ctx, purchase.ID); err != nil {
				failures.Add(1)
				t.Errorf("CancelPurchase() = %v", err)
			}
		}()
	}

	close(start)
	wg.Wait()

	if available := availability(t, s, testCampaign); available != 10 {
		t.Errorf("available = %d after %d simultaneous cancellations, want 10", available, attempts)
	}
}

// A cancelled buyer may buy again, which is the whole reason cancellation moves
// the row rather than deleting it and why the unique index excludes cancelled
// rows.
func TestACancelledBuyerCanBuyAgain(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	openCampaign(t, s, 10)

	first := sell(t, s, "student-1")
	if _, err := s.CancelPurchase(ctx, first.ID); err != nil {
		t.Fatalf("CancelPurchase() = %v", err)
	}

	second, err := s.RecordPending(ctx, testCampaign, "student-1", uuid.NewString())
	if err != nil {
		t.Fatalf("second RecordPending() = %v, want success after a cancellation", err)
	}
	if second.ID == first.ID {
		t.Error("the second purchase reused the first one's id")
	}

	// And the history of the first attempt survives.
	cancelled, err := s.ReadPurchase(ctx, testCampaign, first.ID)
	if err != nil {
		t.Fatalf("ReadPurchase() = %v", err)
	}
	if cancelled.Status != domain.StatusCancelled {
		t.Errorf("the first purchase is %s, want it kept as %s",
			cancelled.Status, domain.StatusCancelled)
	}
}

// Settling is idempotent, which is what makes at-least-once delivery survivable
// in the worker.
func TestSettlingTwiceIsNotAnError(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	openCampaign(t, s, 10)

	purchase := sell(t, s, "student-1")

	first, changed, err := s.SettlePurchase(ctx, purchase.ID, domain.StatusConfirmed)
	if err != nil {
		t.Fatalf("first SettlePurchase() = %v", err)
	}
	if !changed {
		t.Error("the first settle reported no change; it moved the row")
	}

	second, changed, err := s.SettlePurchase(ctx, purchase.ID, domain.StatusConfirmed)
	if err != nil {
		t.Fatalf("second SettlePurchase() = %v, want a duplicate to be accepted", err)
	}
	// The flag is what lets the worker count duplicates apart from first
	// confirmations without comparing two machines' clocks.
	if changed {
		t.Error("the second settle claimed to have changed something")
	}

	if first.Status != domain.StatusConfirmed || second.Status != domain.StatusConfirmed {
		t.Errorf("statuses were %s then %s, want %s twice",
			first.Status, second.Status, domain.StatusConfirmed)
	}
	// The ticket did not move: settling is a change of state, not of stock.
	if available := availability(t, s, testCampaign); available != 9 {
		t.Errorf("available = %d after settling twice, want 9", available)
	}
}

// Settling something that is not pending is a real inconsistency, told apart
// from a duplicate so the worker can stop rather than retry.
func TestSettlingACancelledPurchaseIsRefused(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	openCampaign(t, s, 10)

	purchase := sell(t, s, "student-1")
	if _, err := s.CancelPurchase(ctx, purchase.ID); err != nil {
		t.Fatalf("CancelPurchase() = %v", err)
	}

	_, _, err := s.SettlePurchase(ctx, purchase.ID, domain.StatusConfirmed)
	if !errors.Is(err, domain.ErrPurchaseNotPending) {
		t.Errorf("SettlePurchase() on a cancelled row = %v, want %v", err, domain.ErrPurchaseNotPending)
	}
}

func TestSettlingAPurchaseThatDoesNotExistIsRefused(t *testing.T) {
	s := newTestStore(t)
	openCampaign(t, s, 10)

	_, _, err := s.SettlePurchase(context.Background(), uuid.NewString(), domain.StatusConfirmed)
	if !errors.Is(err, domain.ErrPurchaseNotFound) {
		t.Errorf("SettlePurchase() on a missing row = %v, want %v", err, domain.ErrPurchaseNotFound)
	}
}

// A pending ticket counts against the stock exactly like a confirmed one.
//
// This is the assumption the whole phase 3 design rests on: reconciliation
// reads this table to decide how much stock is left, so a sale it cannot see
// until the worker catches up is a sale a restart would hand out again.
func TestAPendingTicketAlreadyCountsAsSold(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	openCampaign(t, s, 10)

	sell(t, s, "student-1")

	live, err := s.CountLiveTickets(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountLiveTickets() = %v", err)
	}
	if live != 1 {
		t.Errorf("%d live tickets while the purchase is pending, want 1", live)
	}

	confirmed, err := s.CountByStatus(ctx, testCampaign, domain.StatusConfirmed)
	if err != nil {
		t.Fatalf("CountByStatus() = %v", err)
	}
	if confirmed != 0 {
		t.Errorf("%d confirmed tickets before the worker ran, want 0", confirmed)
	}

	// And reconciliation would see it: the buyer is in the list it rebuilds
	// Redis from, so a restart now does not resell this seat.
	state, err := s.ReadCampaignState(ctx, testCampaign)
	if err != nil {
		t.Fatalf("ReadCampaignState() = %v", err)
	}
	if len(state.Buyers) != 1 {
		t.Errorf("reconciliation would see %d buyers, want 1", len(state.Buyers))
	}
	if state.Remaining() != 9 {
		t.Errorf("reconciliation would write %d as the stock, want 9", state.Remaining())
	}
}

// The same idempotency key cannot create two purchases, whatever Redis has
// forgotten.
func TestAnIdempotencyKeyCannotCreateTwoPurchases(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	openCampaign(t, s, 10)

	key := uuid.NewString()
	if _, err := s.RecordPending(ctx, testCampaign, "student-1", key); err != nil {
		t.Fatalf("first RecordPending() = %v", err)
	}

	// A different person, so that the fairness index is not what refuses it —
	// this has to be the idempotency index, and the error has to say so.
	_, err := s.RecordPending(ctx, testCampaign, "student-2", key)
	if !errors.Is(err, domain.ErrIdempotencyKeyReplayed) {
		t.Errorf("reusing a key = %v, want %v", err, domain.ErrIdempotencyKeyReplayed)
	}

	live, err := s.CountLiveTickets(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountLiveTickets() = %v", err)
	}
	if live != 1 {
		t.Errorf("%d live tickets after a reused key, want 1", live)
	}
}

// Reversing a sale has to give the key back with the ticket.
//
// The API answers a failed sale with a 5xx and releases the Idempotency-Key so
// the caller can send the same request again — that is what a 5xx means and
// what the key is for. If a cancelled row went on holding its key, that retry
// would pass the Lua script, reach here, and be refused for a purchase that no
// longer exists; the caller would be locked out of the only key the system can
// recognise, permanently, for having followed the documented protocol.
func TestACancelledPurchaseGivesUpItsIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	openCampaign(t, s, 10)

	key := uuid.NewString()
	first, err := s.RecordPending(ctx, testCampaign, "student-1", key)
	if err != nil {
		t.Fatalf("first RecordPending() = %v", err)
	}

	// The sale is reversed: the publish was refused, or the worker gave up
	// and the compensation saga ran. Either way the seat is back on the shelf
	// and the buyer is free to try again.
	if _, err := s.CancelPurchase(ctx, first.ID); err != nil {
		t.Fatalf("CancelPurchase() = %v", err)
	}

	second, err := s.RecordPending(ctx, testCampaign, "student-1", key)
	if err != nil {
		t.Fatalf("retrying with the same key after the sale was reversed = %v, want it to succeed", err)
	}
	if second.ID == first.ID {
		t.Error("the retry reused the cancelled purchase's id")
	}

	// And the protection the index exists for is untouched: the key that now
	// belongs to a live purchase cannot produce a second one.
	if _, err := s.RecordPending(ctx, testCampaign, "student-2", key); !errors.Is(err, domain.ErrIdempotencyKeyReplayed) {
		t.Errorf("reusing the key while it holds a live purchase = %v, want %v",
			err, domain.ErrIdempotencyKeyReplayed)
	}

	live, err := s.CountLiveTickets(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountLiveTickets() = %v", err)
	}
	if live != 1 {
		t.Errorf("%d live tickets after a reversal and a retry, want 1", live)
	}
}
