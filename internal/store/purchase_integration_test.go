//go:build integration

package store

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
)

const testCampaign = "queima-test"

func TestASinglePurchaseIsRecordedAndDecrementsTheStock(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	if err := store.EnsureCampaign(ctx, testCampaign, 10); err != nil {
		t.Fatalf("EnsureCampaign() = %v", err)
	}

	purchase, err := store.PurchaseNaively(ctx, testCampaign, "student-1")
	if err != nil {
		t.Fatalf("PurchaseNaively() = %v, want no error", err)
	}
	if purchase.UserID != "student-1" || purchase.Status != domain.StatusConfirmed {
		t.Errorf("purchase = %+v, want a confirmed ticket for student-1", purchase)
	}
	if purchase.CreatedAt.IsZero() {
		t.Error("purchase has no creation time; the database default did not come back")
	}

	sold, err := store.CountByStatus(ctx, testCampaign, domain.StatusConfirmed)
	if err != nil {
		t.Fatalf("CountByStatus() = %v", err)
	}
	if sold != 1 {
		t.Errorf("confirmed purchases = %d, want 1", sold)
	}
	if available := availability(t, store, testCampaign); available != 9 {
		t.Errorf("available = %d, want 9", available)
	}
}

func TestPurchasesAreRefusedOnceTheStockIsGone(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	if err := store.EnsureCampaign(ctx, testCampaign, 3); err != nil {
		t.Fatalf("EnsureCampaign() = %v", err)
	}

	// One request at a time, which is the case the naive implementation
	// does handle. The concurrent case is the point of the test below.
	for i := range 3 {
		if _, err := store.PurchaseNaively(ctx, testCampaign, userID(i)); err != nil {
			t.Fatalf("purchase %d = %v, want no error", i, err)
		}
	}

	_, err := store.PurchaseNaively(ctx, testCampaign, "one-too-many")
	if !errors.Is(err, domain.ErrSoldOut) {
		t.Errorf("fourth purchase = %v, want %v", err, domain.ErrSoldOut)
	}
}

func TestTheSameUserIsRefusedASecondTicket(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	if err := store.EnsureCampaign(ctx, testCampaign, 10); err != nil {
		t.Fatalf("EnsureCampaign() = %v", err)
	}

	if _, err := store.PurchaseNaively(ctx, testCampaign, "student-1"); err != nil {
		t.Fatalf("first purchase = %v", err)
	}

	_, err := store.PurchaseNaively(ctx, testCampaign, "student-1")
	if !errors.Is(err, domain.ErrAlreadyPurchased) {
		t.Errorf("second purchase = %v, want %v", err, domain.ErrAlreadyPurchased)
	}
}

func TestPurchasingFromACampaignThatDoesNotExistIsRefused(t *testing.T) {
	store := newTestStore(t)

	_, err := store.PurchaseNaively(context.Background(), "no-such-campaign", "student-1")
	if !errors.Is(err, domain.ErrCampaignNotFound) {
		t.Errorf("purchase = %v, want %v", err, domain.ErrCampaignNotFound)
	}
}

// Every process runs EnsureCampaign at startup. If it wrote available = total
// each time, a restart, a crash or a rolling deploy would refill the stock
// mid-campaign and start selling tickets that are already in someone's hands.
func TestEnsureCampaignNeverRefillsTheStock(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	if err := store.EnsureCampaign(ctx, testCampaign, 10); err != nil {
		t.Fatalf("EnsureCampaign() = %v", err)
	}
	for i := range 4 {
		if _, err := store.PurchaseNaively(ctx, testCampaign, userID(i)); err != nil {
			t.Fatalf("purchase %d = %v", i, err)
		}
	}

	// The restart.
	if err := store.EnsureCampaign(ctx, testCampaign, 10); err != nil {
		t.Fatalf("second EnsureCampaign() = %v", err)
	}

	if available := availability(t, store, testCampaign); available != 6 {
		t.Errorf("available = %d after restart, want 6; the stock was refilled", available)
	}
}

// The demonstration this phase exists for.
//
// Between reading `available` and decrementing it, every other request reads
// the same value and reaches the same conclusion, so far more tickets are sold
// than exist. The test asserts the failure because the whole phase 1 to phase 2
// argument rests on it: if this ever stops reproducing, the comparison in
// docs/DECISIONS.md is measuring nothing and should fail loudly rather than
// quietly pass.
func TestTheNaivePathOversellsUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	const (
		stock      = 20
		contenders = 300
	)

	if err := store.EnsureCampaign(ctx, testCampaign, stock); err != nil {
		t.Fatalf("EnsureCampaign() = %v", err)
	}

	// A barrier so every goroutine attempts its read at the same moment
	// rather than trickling in as the scheduler starts them.
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			//nolint:errcheck // refusals are expected; the counts below are the assertion
			_, _ = store.PurchaseNaively(ctx, testCampaign, userID(i))
		}()
	}

	close(start)
	wg.Wait()

	sold, err := store.CountByStatus(ctx, testCampaign, domain.StatusConfirmed)
	if err != nil {
		t.Fatalf("CountByStatus() = %v", err)
	}

	t.Logf("stock was %d, %d requests arrived, %d tickets were sold (%d oversold)",
		stock, contenders, sold, sold-stock)

	if sold <= stock {
		t.Fatalf("sold %d of %d tickets: the race did not reproduce, so the "+
			"phase 1 baseline is not measuring what it claims", sold, stock)
	}
}

// The phase 1 baseline, measured over repeated runs rather than once.
//
// The brief is specific about why: a single print showing an oversell "pode ter
// sido sorte", and the quantified proof it asks for is N runs with the excess
// printed for each. One run cannot distinguish a race that always loses from a
// race that lost once — and the whole comparison table in docs/DECISIONS.md
// rests on this number being characteristic rather than lucky.
//
// So this runs the same contest repeatedly against a campaign reset in between,
// prints the table, and fails if *any* run failed to oversell. That last part is
// what makes it a regression test rather than a demonstration: if the naive path
// ever stops racing — a Postgres version that locks differently, a machine too
// slow to interleave — the baseline is measuring nothing, and the comparison it
// anchors should go red rather than quietly pass.
func TestTheNaivePathOversellsEveryTime(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	const (
		stock      = 20
		contenders = 300
		runs       = 5
	)

	if err := store.EnsureCampaign(ctx, testCampaign, stock); err != nil {
		t.Fatalf("EnsureCampaign() = %v", err)
	}

	oversold := make([]int, 0, runs)

	for run := 1; run <= runs; run++ {
		resetCampaign(t, store, stock)

		start := make(chan struct{})
		var wg sync.WaitGroup

		for i := range contenders {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				//nolint:errcheck // refusals are expected; the count below is the assertion
				_, _ = store.PurchaseNaively(ctx, testCampaign, userID(i))
			}()
		}

		close(start)
		wg.Wait()

		sold, err := store.CountByStatus(ctx, testCampaign, domain.StatusConfirmed)
		if err != nil {
			t.Fatalf("run %d: CountByStatus() = %v", run, err)
		}
		oversold = append(oversold, sold-stock)
	}

	t.Logf("%d runs of %d requests against %d tickets", runs, contenders, stock)
	t.Log("  run | sold | oversold")
	for i, excess := range oversold {
		t.Logf("  %3d | %4d | %8d", i+1, excess+stock, excess)
	}

	for i, excess := range oversold {
		if excess <= 0 {
			t.Errorf("run %d sold %d of %d tickets: the race did not reproduce, so the "+
				"phase 1 baseline is not measuring what it claims", i+1, excess+stock, stock)
		}
	}
}

// resetCampaign returns the campaign to the state a run starts in.
//
// Cheaper than a container per run, and it has to be exact: a run that began
// with the counter already negative would report an oversell it did not cause.
func resetCampaign(t *testing.T, s *Store, stock int) {
	t.Helper()

	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `DELETE FROM purchases WHERE campaign_id = $1`, testCampaign); err != nil {
		t.Fatalf("clearing purchases: %v", err)
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE tickets SET available = $2 WHERE campaign_id = $1`, testCampaign, stock); err != nil {
		t.Fatalf("restoring the counter: %v", err)
	}
}

// availability reads the counter directly, which is deliberately not something
// the production API exposes.
func availability(t *testing.T, s *Store, campaignID string) int {
	t.Helper()

	var available int
	if err := s.pool.QueryRow(context.Background(),
		`SELECT available FROM tickets WHERE campaign_id = $1`, campaignID,
	).Scan(&available); err != nil {
		t.Fatalf("reading availability: %v", err)
	}
	return available
}

// userID produces a distinct identity per contender, so that a request refused
// as "already purchased" is a genuine signal rather than an artefact of the
// test reusing one account.
func userID(i int) string {
	return "student-" + strconv.Itoa(i)
}
