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

	sold, err := store.CountConfirmed(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountConfirmed() = %v", err)
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
// argument rests on it: if this ever stops reproducing, the comparison in the
// README is measuring nothing and should fail loudly rather than quietly pass.
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

	sold, err := store.CountConfirmed(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountConfirmed() = %v", err)
	}

	t.Logf("stock was %d, %d requests arrived, %d tickets were sold (%d oversold)",
		stock, contenders, sold, sold-stock)

	if sold <= stock {
		t.Fatalf("sold %d of %d tickets: the race did not reproduce, so the "+
			"phase 1 baseline is not measuring what it claims", sold, stock)
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
