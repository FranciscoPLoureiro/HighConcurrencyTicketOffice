//go:build integration

package purchase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
)

// The test phase 5 exists to pass.
//
// Kill a sale between the decrement and the record — the exact window the brief
// describes, in which "the ticket was taken out of stock, will never be
// processed, and the client never gets a reply" — and prove that the stock
// comes back and nothing is lost.
//
// The fault is injected rather than performed: an in-process test cannot call
// os.Exit and then assert anything. What it leaves behind is identical, and
// that is the only thing the sweeper can see — a reservation open, no row in
// PostgreSQL, and nothing anywhere that intends to finish the job.
func TestATicketStrandedByACrashComesBack(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.openCampaign(t, 10)

	// Everything is settled the instant it is examined, so the test does not
	// spend a minute proving a duration it already trusts. The age is a
	// safety margin against a *live* request, and there is no live request
	// here — the sale it belonged to is over.
	h.service.WithSweepPolicy(SweepPolicy{ReservationAge: time.Nanosecond, PendingAge: time.Nanosecond})
	h.service.WithFaults(Faults{Armed: FaultAfterDecrement})

	_, err := h.service.Purchase(ctx, testCampaign, "student-1", newKey())
	if !errors.Is(err, ErrFaultInjected) {
		t.Fatalf("Purchase() = %v, want the injected fault", err)
	}

	// The damage, before anything has repaired it: the ticket is gone from
	// the shelf and no record of it exists.
	remaining, _, err := h.cache.Remaining(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Remaining() = %v", err)
	}
	if remaining != 9 {
		t.Fatalf("stock is %d after the crash, want 9 — the fault did not open the window", remaining)
	}
	if live, err := h.store.CountLiveTickets(ctx, testCampaign); err != nil || live != 0 {
		t.Fatalf("postgres holds %d tickets (err %v), want 0", live, err)
	}
	if held, err := h.cache.HoldsTicket(ctx, testCampaign, "student-1"); err != nil || !held {
		t.Fatalf("redis says student-1 holds no ticket (err %v) — nothing was stranded", err)
	}

	// Nobody is coming back for it, so the sweeper gives it up.
	h.service.WithFaults(Faults{})
	result, err := h.service.Sweep(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Sweep() = %v", err)
	}
	if result.Released != 1 {
		t.Fatalf("sweep released %d tickets, want 1 (examined %d, closed %d)",
			result.Released, result.Examined, result.Closed)
	}

	// The stock is whole again.
	remaining, _, err = h.cache.Remaining(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Remaining() = %v", err)
	}
	if remaining != 10 {
		t.Errorf("stock is %d after the sweep, want 10", remaining)
	}
	if open, err := h.service.Reserved(ctx, testCampaign); err != nil || open != 0 {
		t.Errorf("%d reservations still open (err %v), want 0", open, err)
	}

	// And the person it was stranded on can buy again, which is the half that
	// would be missed by only checking the counter.
	if _, err := h.service.Purchase(ctx, testCampaign, "student-1", newKey()); err != nil {
		t.Errorf("purchase after the sweep = %v, want success", err)
	}
}

// The sweeper must not take back a ticket that was actually sold.
//
// This is the failure mode the brief warns about — "exige cuidado para não
// devolver um bilhete que foi mesmo processado" — and the one that would sell a
// seat twice. Every reservation here belongs to a completed sale, so a correct
// sweep releases nothing at all.
func TestASweepNeverTakesBackACompletedSale(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.openCampaign(t, 10)

	for i := range 5 {
		if _, err := h.service.Purchase(ctx, testCampaign, student(i), newKey()); err != nil {
			t.Fatalf("Purchase() = %v", err)
		}
	}

	// Every reservation is instantly eligible, which is the most hostile
	// setting this can be given: nothing is protected by not having aged yet.
	h.service.WithSweepPolicy(SweepPolicy{ReservationAge: time.Nanosecond, PendingAge: time.Hour})

	result, err := h.service.Sweep(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Sweep() = %v", err)
	}
	if result.Released != 0 {
		t.Errorf("sweep released %d tickets that were properly sold — this is an oversell",
			result.Released)
	}

	remaining, _, err := h.cache.Remaining(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Remaining() = %v", err)
	}
	if remaining != 5 {
		t.Errorf("stock is %d after sweeping five real sales, want 5", remaining)
	}
	if live, err := h.store.CountLiveTickets(ctx, testCampaign); err != nil || live != 5 {
		t.Errorf("postgres holds %d tickets (err %v), want 5", live, err)
	}
}

// Sweeping twice returns one ticket, not two.
//
// The brief's warning about compensation running twice, arriving through a
// different door: two sweepers, or one restarted mid-pass, must not each credit
// the stock. The guard is in release.lua, where the test and the increment are
// one step.
func TestSweepingTwiceReturnsOneTicket(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.openCampaign(t, 10)

	h.service.WithSweepPolicy(SweepPolicy{ReservationAge: time.Nanosecond, PendingAge: time.Nanosecond})
	h.service.WithFaults(Faults{Armed: FaultAfterDecrement})

	if _, err := h.service.Purchase(ctx, testCampaign, "student-1", newKey()); !errors.Is(err, ErrFaultInjected) {
		t.Fatalf("Purchase() = %v, want the injected fault", err)
	}
	h.service.WithFaults(Faults{})

	for range 3 {
		if _, err := h.service.Sweep(ctx, testCampaign); err != nil {
			t.Fatalf("Sweep() = %v", err)
		}
	}

	remaining, _, err := h.cache.Remaining(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Remaining() = %v", err)
	}
	if remaining != 10 {
		t.Errorf("stock is %d after sweeping the same ticket three times, want 10", remaining)
	}
}

// A sale killed after its row was written is republished, not released.
//
// The second window. The row is a real sale and the seat is genuinely taken —
// what is missing is a message — so giving the ticket back would be wrong and
// the fix is to ask for the work again.
func TestASaleStrandedAfterItsRowIsRepublished(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.openCampaign(t, 10)

	h.service.WithSweepPolicy(SweepPolicy{ReservationAge: time.Nanosecond, PendingAge: time.Nanosecond})
	h.service.WithFaults(Faults{Armed: FaultAfterRecord})

	_, err := h.service.Purchase(ctx, testCampaign, "student-1", newKey())
	if !errors.Is(err, ErrFaultInjected) {
		t.Fatalf("Purchase() = %v, want the injected fault", err)
	}

	// The row exists and nothing was ever queued for it.
	if live, err := h.store.CountLiveTickets(ctx, testCampaign); err != nil || live != 1 {
		t.Fatalf("postgres holds %d tickets (err %v), want 1", live, err)
	}
	if h.published.count() != 0 {
		t.Fatalf("%d messages were published, want 0 — the fault did not open the window",
			h.published.count())
	}

	h.service.WithFaults(Faults{})
	result, err := h.service.Sweep(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Sweep() = %v", err)
	}

	if result.Republished != 1 {
		t.Errorf("republished %d stalled sales, want 1", result.Republished)
	}
	if result.Released != 0 {
		t.Errorf("released %d tickets that were genuinely sold, want 0", result.Released)
	}
	if h.published.count() != 1 {
		t.Errorf("%d messages published after the sweep, want 1", h.published.count())
	}

	// The seat stays taken throughout, which is the point.
	remaining, _, err := h.cache.Remaining(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Remaining() = %v", err)
	}
	if remaining != 9 {
		t.Errorf("stock is %d, want 9 — the seat is sold and must stay sold", remaining)
	}
}

// The campaign survives a run in which a crash strands every other sale.
//
// The brief's scenario stated as a test: a hundred tickets and enough
// well-placed crashes to empty the shelf without selling anything. Afterwards
// every stranded ticket must be back and the campaign must still be able to
// sell exactly its remaining stock — no more.
func TestACampaignFullOfCrashesLosesNoTickets(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	const stock = 20
	h.openCampaign(t, stock)
	h.service.WithSweepPolicy(SweepPolicy{ReservationAge: time.Nanosecond, PendingAge: time.Hour})

	// Every other sale dies in the first window.
	var crashed, sold int
	for i := range stock {
		if i%2 == 0 {
			h.service.WithFaults(Faults{Armed: FaultAfterDecrement})
		} else {
			h.service.WithFaults(Faults{})
		}

		switch _, err := h.service.Purchase(ctx, testCampaign, student(i), newKey()); {
		case errors.Is(err, ErrFaultInjected):
			crashed++
		case err == nil:
			sold++
		default:
			t.Fatalf("purchase by %s = %v", student(i), err)
		}
	}
	h.service.WithFaults(Faults{})

	if crashed != stock/2 || sold != stock/2 {
		t.Fatalf("%d crashed and %d sold, want %d of each", crashed, sold, stock/2)
	}

	// Before the sweep the shelf is empty and only half the tickets exist.
	if remaining, _, _ := h.cache.Remaining(ctx, testCampaign); remaining != 0 {
		t.Fatalf("stock is %d before the sweep, want 0", remaining)
	}

	if _, err := h.service.Sweep(ctx, testCampaign); err != nil {
		t.Fatalf("Sweep() = %v", err)
	}

	// Every stranded ticket is back, and only those.
	remaining, _, err := h.cache.Remaining(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Remaining() = %v", err)
	}
	if remaining != int64(crashed) {
		t.Errorf("stock is %d after the sweep, want %d", remaining, crashed)
	}

	// And the campaign still cannot oversell: exactly the recovered tickets
	// are available, and the next buyer after them is refused.
	for i := range crashed {
		if _, err := h.service.Purchase(ctx, testCampaign, student(100+i), newKey()); err != nil {
			t.Fatalf("purchase %d of the recovered tickets = %v", i+1, err)
		}
	}
	if _, err := h.service.Purchase(ctx, testCampaign, student(999), newKey()); !errors.Is(err, domain.ErrSoldOut) {
		t.Errorf("one past the recovered stock = %v, want %v", err, domain.ErrSoldOut)
	}

	live, err := h.store.CountLiveTickets(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountLiveTickets() = %v", err)
	}
	if live != stock {
		t.Errorf("postgres holds %d tickets, want exactly %d", live, stock)
	}
}
