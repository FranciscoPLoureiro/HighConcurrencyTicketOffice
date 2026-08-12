//go:build integration

package purchase

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/correlation"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/queue"
)

// The test the whole phase exists to pass.
//
// Five hundred people press Buy at the same instant against one hundred
// tickets. Exactly one hundred may come out, nobody may hold two, and the two
// systems must agree about it afterwards. Phase 1's implementation sold all
// five hundred; if this ever sells one hundred and one, the Lua script has
// stopped being atomic and everything downstream of it is decoration.
func TestFiveHundredBuyersAgainstOneHundredTicketsSellExactlyOneHundred(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	const (
		stock      = 100
		contenders = 500
	)
	h.openCampaign(t, stock)

	var sold, refusedSoldOut, other atomic.Int64

	// A barrier, so every goroutine arrives at the script together rather
	// than trickling in as the scheduler starts them.
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			switch _, err := h.service.Purchase(ctx, testCampaign, student(i), newKey()); {
			case err == nil:
				sold.Add(1)
			case errors.Is(err, domain.ErrSoldOut):
				refusedSoldOut.Add(1)
			default:
				other.Add(1)
				t.Errorf("purchase by %s = %v, want success or sold out", student(i), err)
			}
		}()
	}

	close(start)
	wg.Wait()

	t.Logf("%d requests against %d tickets: %d sold, %d refused as sold out, %d other",
		contenders, stock, sold.Load(), refusedSoldOut.Load(), other.Load())

	if sold.Load() != stock {
		t.Errorf("sold %d tickets, want exactly %d", sold.Load(), stock)
	}
	if refusedSoldOut.Load() != contenders-stock {
		t.Errorf("refused %d as sold out, want %d", refusedSoldOut.Load(), contenders-stock)
	}

	// What Redis believes.
	remaining, existed, err := h.cache.Remaining(ctx, testCampaign)
	if err != nil || !existed {
		t.Fatalf("Remaining() = %d, %t, %v", remaining, existed, err)
	}
	if remaining != 0 {
		t.Errorf("redis has %d tickets left, want 0", remaining)
	}
	if holders, err := h.cache.CountHolders(ctx, testCampaign); err != nil || holders != stock {
		t.Errorf("redis has %d ticket holders (err %v), want %d", holders, err, stock)
	}

	// What Postgres believes. The two have to agree, or the invariant only
	// held in the system that cannot be restarted.
	confirmed, err := h.store.CountLiveTickets(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountLiveTickets() = %v", err)
	}
	if confirmed != stock {
		t.Errorf("postgres recorded %d purchases, want %d", confirmed, stock)
	}

	offenders, err := h.store.CountUsersWithMultipleTickets(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountUsersWithMultipleTickets() = %v", err)
	}
	if offenders != 0 {
		t.Errorf("%d users hold more than one ticket, want 0", offenders)
	}

	state, err := h.store.ReadCampaignState(ctx, testCampaign)
	if err != nil {
		t.Fatalf("ReadCampaignState() = %v", err)
	}
	if !state.Consistent() {
		t.Errorf("postgres counter says %d available, the rows say %d",
			state.Available, state.Total-len(state.Buyers))
	}
}

// The fairness rule under a race with itself.
//
// One person, many tabs, all submitted together. The duplicate check and the
// decrement are in the same atomic step precisely so this cannot come out as
// two tickets — checking eligibility and taking stock in two round trips
// reintroduces the phase 1 race with a different variable.
func TestOnePersonRacingThemselvesGetsOneTicket(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.openCampaign(t, 100)

	const attempts = 50

	var sold, refusedDuplicate atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup

	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			switch _, err := h.service.Purchase(ctx, testCampaign, "eager-student", newKey()); {
			case err == nil:
				sold.Add(1)
			case errors.Is(err, domain.ErrAlreadyPurchased):
				refusedDuplicate.Add(1)
			default:
				t.Errorf("purchase = %v, want success or already purchased", err)
			}
		}()
	}

	close(start)
	wg.Wait()

	if sold.Load() != 1 {
		t.Errorf("the same person got %d tickets from %d simultaneous attempts, want 1", sold.Load(), attempts)
	}
	if refusedDuplicate.Load() != attempts-1 {
		t.Errorf("%d attempts were refused as duplicates, want %d", refusedDuplicate.Load(), attempts-1)
	}

	// And the campaign only lost one ticket to them.
	remaining, _, err := h.cache.Remaining(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Remaining() = %v", err)
	}
	if remaining != 99 {
		t.Errorf("redis has %d tickets left, want 99", remaining)
	}
}

func TestASoldOutCampaignStillTellsARepeatBuyerWhyTheyWereRefused(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.openCampaign(t, 1)

	if _, err := h.service.Purchase(ctx, testCampaign, "student-1", newKey()); err != nil {
		t.Fatalf("first purchase = %v", err)
	}

	// The campaign is now empty, so both refusals apply to this caller. The
	// more specific one has to win, or the answer to "why was I refused?"
	// depends on the order the script happened to check things in.
	_, err := h.service.Purchase(ctx, testCampaign, "student-1", newKey())
	if !errors.Is(err, domain.ErrAlreadyPurchased) {
		t.Errorf("second purchase by the holder = %v, want %v", err, domain.ErrAlreadyPurchased)
	}

	_, err = h.service.Purchase(ctx, testCampaign, "student-2", newKey())
	if !errors.Is(err, domain.ErrSoldOut) {
		t.Errorf("purchase by a newcomer = %v, want %v", err, domain.ErrSoldOut)
	}
}

// A campaign Redis has never heard of is not a sold out campaign.
//
// Answering "no tickets left" to a missing stock key is a lie that looks like a
// normal, final refusal — the caller stops trying and nobody investigates.
func TestAnUnreconciledCampaignIsNotReportedAsSoldOut(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	if err := h.store.EnsureCampaign(ctx, testCampaign, 100); err != nil {
		t.Fatalf("EnsureCampaign() = %v", err)
	}
	// Deliberately no reconciliation: this is a process that skipped
	// startup, or a Redis that was flushed underneath a running one.

	_, err := h.service.Purchase(ctx, testCampaign, "student-1", newKey())
	if !errors.Is(err, domain.ErrCampaignNotFound) {
		t.Errorf("purchase against an unreconciled campaign = %v, want %v", err, domain.ErrCampaignNotFound)
	}
}

// A sale is not finished when the row is written; it is finished when somebody
// else has been told to fulfil it.
//
// Without this assertion the whole phase could be inert — rows piling up as
// pending, no message ever published, and every other test in this file still
// passing because they only look at Redis and PostgreSQL.
func TestEverySoldTicketIsHandedOverForFulfilment(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.openCampaign(t, 10)

	const contenders = 25
	var sold int

	for i := range contenders {
		if _, err := h.service.Purchase(ctx, testCampaign, student(i), newKey()); err == nil {
			sold++
		}
	}

	if sold != 10 {
		t.Fatalf("sold %d tickets against a stock of 10", sold)
	}
	if queued := h.published.count(); queued != sold {
		t.Errorf("%d tickets were sold and %d were queued for fulfilment", sold, queued)
	}
}

// The message carries what the worker needs to do its job and to be found in
// the logs afterwards.
func TestTheQueuedMessageCarriesTheKeysThatIdentifyThePurchase(t *testing.T) {
	ctx := correlation.WithID(context.Background(), "correlation-under-test")
	h := newHarness(t)
	h.openCampaign(t, 1)

	key := newKey()
	purchase, err := h.service.Purchase(ctx, testCampaign, "student-1", key)
	if err != nil {
		t.Fatalf("Purchase() = %v", err)
	}

	sent := h.published.messages()
	if len(sent) != 1 {
		t.Fatalf("%d messages were published, want 1", len(sent))
	}

	switch {
	case sent[0].PurchaseID != purchase.ID:
		t.Errorf("message names purchase %q, want %q", sent[0].PurchaseID, purchase.ID)
	case sent[0].IdempotencyKey != key:
		t.Errorf("message carries key %q, want %q", sent[0].IdempotencyKey, key)
	case sent[0].CorrelationID != "correlation-under-test":
		t.Errorf("message carries correlation id %q, want the request's", sent[0].CorrelationID)
	}
}

// A publish the broker definitively refused undoes the sale.
//
// The ticket was decremented and the row written, and nothing is ever going to
// fulfil it. Leaving it alone would quietly end the campaign one ticket short
// for every refused publish, and leave a person holding a purchase that never
// progresses past pending.
func TestASaleTheBrokerRefusedIsUndoneCompletely(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.openCampaign(t, 5)

	h.published.fail(fmt.Errorf("%w: disk alarm", queue.ErrPublishRefused))

	if _, err := h.service.Purchase(ctx, testCampaign, "student-1", newKey()); err == nil {
		t.Fatal("Purchase() = nil, want the publish failure to surface")
	}

	// The ticket is back on the shelf.
	remaining, _, err := h.cache.Remaining(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Remaining() = %v", err)
	}
	if remaining != 5 {
		t.Errorf("redis has %d tickets after a refused publish, want 5", remaining)
	}

	// PostgreSQL agrees, both in the rows and in the counter beside them.
	live, err := h.store.CountLiveTickets(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountLiveTickets() = %v", err)
	}
	if live != 0 {
		t.Errorf("%d live tickets after a refused publish, want 0", live)
	}

	state, err := h.store.ReadCampaignState(ctx, testCampaign)
	if err != nil {
		t.Fatalf("ReadCampaignState() = %v", err)
	}
	if !state.Consistent() {
		t.Errorf("counter says %d available, the rows say %d",
			state.Available, state.Total-len(state.Buyers))
	}

	// And the buyer is free to try again, which is the point of undoing it.
	h.published.fail(nil)
	if _, err := h.service.Purchase(ctx, testCampaign, "student-1", newKey()); err != nil {
		t.Errorf("purchase after the reversal = %v, want success", err)
	}
}

// A publish that was never confirmed is left alone.
//
// The broker may have taken the message and been slow to say so. Undoing here
// would cancel a purchase a worker is about to fulfil, which is the same
// mistake as compensating an unknown commit and produces the same result: two
// people in one seat.
func TestASaleWithAnUnconfirmedPublishIsLeftPending(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.openCampaign(t, 5)

	h.published.fail(fmt.Errorf("%w: %w", queue.ErrPublishUnconfirmed, context.DeadlineExceeded))

	if _, err := h.service.Purchase(ctx, testCampaign, "student-1", newKey()); err == nil {
		t.Fatal("Purchase() = nil, want the publish failure to surface")
	}

	// The ticket stays out of circulation: undersold by one, which is the
	// recoverable direction.
	remaining, _, err := h.cache.Remaining(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Remaining() = %v", err)
	}
	if remaining != 4 {
		t.Errorf("redis has %d tickets after an unconfirmed publish, want 4", remaining)
	}

	live, err := h.store.CountLiveTickets(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountLiveTickets() = %v", err)
	}
	if live != 1 {
		t.Errorf("%d live tickets after an unconfirmed publish, want 1 — "+
			"cancelling here can undo a sale that is about to be fulfilled", live)
	}
}
