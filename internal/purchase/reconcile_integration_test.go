//go:build integration

package purchase

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
)

// The brief's test, run exactly as written: sell forty, kill the process, start
// again, and check that Redis says sixty rather than one hundred.
//
// The failure this guards against is the one-line version of startup —
// `SET stock 100` — which is not a simplification but a bug: every deploy,
// crash and OOM kill refills the shelf, and the tickets that come back out of
// it have already been sold to somebody.
func TestARestartDoesNotRefillTheStock(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	const (
		stock = 100
		sold  = 40
	)
	h.openCampaign(t, stock)

	for i := range sold {
		if _, err := h.service.Purchase(ctx, testCampaign, student(i), newKey()); err != nil {
			t.Fatalf("purchase %d = %v", i, err)
		}
	}

	// The crash. Whatever the old process knew is gone; Redis and Postgres
	// are exactly as it left them.
	restarted := h.restart(t)

	if _, err := restarted.Reconcile(ctx, testCampaign); err != nil {
		t.Fatalf("Reconcile() after restart = %v", err)
	}

	remaining, existed, err := h.cache.Remaining(ctx, testCampaign)
	if err != nil || !existed {
		t.Fatalf("Remaining() = %d, %t, %v", remaining, existed, err)
	}
	if remaining != stock-sold {
		t.Fatalf("redis has %d tickets after the restart, want %d; the stock was refilled",
			remaining, stock-sold)
	}
}

// Reconciliation has to survive Redis being wrong, not merely Redis being
// empty.
//
// A cold start reads a missing key and any implementation looks correct. The
// interesting case is a Redis that confidently holds the wrong number — a stale
// AOF, a restored snapshot, or the blind SET this whole mechanism replaces —
// because only an implementation that overwrites from the source of truth
// fixes it.
func TestReconciliationOverwritesAConfidentlyWrongStock(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.openCampaign(t, 100)

	for i := range 40 {
		if _, err := h.service.Purchase(ctx, testCampaign, student(i), newKey()); err != nil {
			t.Fatalf("purchase %d = %v", i, err)
		}
	}

	// The bug, committed on purpose: the shelf is refilled behind the
	// system's back.
	if err := h.cache.SetRemaining(ctx, testCampaign, 100); err != nil {
		t.Fatalf("SetRemaining() = %v", err)
	}

	if _, err := h.restart(t).Reconcile(ctx, testCampaign); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	remaining, _, err := h.cache.Remaining(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Remaining() = %v", err)
	}
	if remaining != 60 {
		t.Errorf("redis has %d tickets after reconciliation, want 60", remaining)
	}
}

// The half of reconciliation the brief does not ask for, and the half that is
// easier to forget because nothing about the stock number looks wrong without
// it.
//
// Restoring the count and losing the buyers gives a correct shelf and no
// memory. Everyone who already bought can buy again, the campaign sells more
// tickets than it has to more people than it should, and the fairness rule
// quietly stops existing halfway through — with a stock counter that looks
// perfectly healthy the whole time.
func TestReconciliationRestoresWhoAlreadyBought(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.openCampaign(t, 100)

	const buyers = 40
	for i := range buyers {
		if _, err := h.service.Purchase(ctx, testCampaign, student(i), newKey()); err != nil {
			t.Fatalf("purchase %d = %v", i, err)
		}
	}

	// A restart that has to rebuild everything, not just the counter.
	if err := h.cache.SetRemaining(ctx, testCampaign, 60); err != nil {
		t.Fatalf("SetRemaining() = %v", err)
	}
	restarted := h.restart(t)
	if _, err := restarted.Reconcile(ctx, testCampaign); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	if holders, err := h.cache.CountHolders(ctx, testCampaign); err != nil || holders != buyers {
		t.Fatalf("redis knows %d ticket holders (err %v), want %d", holders, err, buyers)
	}

	// The behaviour that matters: somebody who already has a ticket is still
	// refused after the restart.
	_, err := restarted.Purchase(ctx, testCampaign, student(0), newKey())
	if !errors.Is(err, domain.ErrAlreadyPurchased) {
		t.Errorf("an existing holder buying again after a restart = %v, want %v",
			err, domain.ErrAlreadyPurchased)
	}

	// And the database's own backstop agrees.
	offenders, err := h.store.CountUsersWithMultipleTickets(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountUsersWithMultipleTickets() = %v", err)
	}
	if offenders != 0 {
		t.Errorf("%d users hold more than one ticket after the restart, want 0", offenders)
	}
}

// A rolling deploy starts several instances at once and every one of them
// reconciles. They must not interleave, and the result must not depend on how
// many of them there were.
func TestConcurrentReconciliationsAgreeOnOneAnswer(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.openCampaign(t, 100)

	for i := range 25 {
		if _, err := h.service.Purchase(ctx, testCampaign, student(i), newKey()); err != nil {
			t.Fatalf("purchase %d = %v", i, err)
		}
	}

	const instances = 8

	var failures atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup

	for range instances {
		service := h.restart(t)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := service.Reconcile(ctx, testCampaign); err != nil {
				failures.Add(1)
				t.Errorf("Reconcile() = %v", err)
			}
		}()
	}

	close(start)
	wg.Wait()

	if failures.Load() != 0 {
		t.Fatalf("%d of %d simultaneous reconciliations failed", failures.Load(), instances)
	}

	remaining, _, err := h.cache.Remaining(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Remaining() = %v", err)
	}
	if remaining != 75 {
		t.Errorf("redis has %d tickets after %d simultaneous reconciliations, want 75", remaining, instances)
	}
	if holders, err := h.cache.CountHolders(ctx, testCampaign); err != nil || holders != 25 {
		t.Errorf("redis knows %d holders (err %v), want 25", holders, err)
	}
}

// Reconciliation runs before the process serves anything, so the state it
// leaves has to be immediately sellable — the remaining stock, and no more.
func TestTheCampaignResumesExactlyWhereItStopped(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.openCampaign(t, 10)

	for i := range 4 {
		if _, err := h.service.Purchase(ctx, testCampaign, student(i), newKey()); err != nil {
			t.Fatalf("purchase %d = %v", i, err)
		}
	}

	restarted := h.restart(t)
	if _, err := restarted.Reconcile(ctx, testCampaign); err != nil {
		t.Fatalf("Reconcile() = %v", err)
	}

	// Six left, and the seventh newcomer is refused.
	for i := 100; i < 106; i++ {
		if _, err := restarted.Purchase(ctx, testCampaign, student(i), newKey()); err != nil {
			t.Fatalf("purchase after restart by %s = %v, want success", student(i), err)
		}
	}

	_, err := restarted.Purchase(ctx, testCampaign, student(200), newKey())
	if !errors.Is(err, domain.ErrSoldOut) {
		t.Errorf("the eleventh ticket = %v, want %v", err, domain.ErrSoldOut)
	}

	confirmed, err := h.store.CountLiveTickets(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountLiveTickets() = %v", err)
	}
	if confirmed != 10 {
		t.Errorf("postgres recorded %d purchases across the restart, want 10", confirmed)
	}
}
