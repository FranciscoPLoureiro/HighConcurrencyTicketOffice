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

			switch _, err := h.service.Purchase(ctx, testCampaign, student(i)); {
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
	confirmed, err := h.store.CountConfirmed(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountConfirmed() = %v", err)
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

			switch _, err := h.service.Purchase(ctx, testCampaign, "eager-student"); {
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

	if _, err := h.service.Purchase(ctx, testCampaign, "student-1"); err != nil {
		t.Fatalf("first purchase = %v", err)
	}

	// The campaign is now empty, so both refusals apply to this caller. The
	// more specific one has to win, or the answer to "why was I refused?"
	// depends on the order the script happened to check things in.
	_, err := h.service.Purchase(ctx, testCampaign, "student-1")
	if !errors.Is(err, domain.ErrAlreadyPurchased) {
		t.Errorf("second purchase by the holder = %v, want %v", err, domain.ErrAlreadyPurchased)
	}

	_, err = h.service.Purchase(ctx, testCampaign, "student-2")
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

	_, err := h.service.Purchase(ctx, testCampaign, "student-1")
	if !errors.Is(err, domain.ErrCampaignNotFound) {
		t.Errorf("purchase against an unreconciled campaign = %v, want %v", err, domain.ErrCampaignNotFound)
	}
}
