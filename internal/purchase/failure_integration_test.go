//go:build integration

package purchase

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/cache"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
)

// Redis disappears mid-campaign. What does the API do?
//
// The brief asks the question and says the answer for a hundred tickets at
// eighty percent off is almost certainly fail-closed. It is, and this proves it
// rather than asserting it — Testcontainers can take the container away, which
// is the only way to find out what the code really does when the one system
// enforcing the stock invariant is gone.
//
// Fail-open would mean selling on the assumption that stock probably remains.
// For a campaign that is oversubscribed fifty to one, "probably" is a hundred
// angry people and a refund process. Refusing everybody is a bad afternoon that
// ends when Redis comes back.
func TestPurchasesAreRefusedWhileRedisIsGone(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.openCampaign(t, 10)

	// A sale before the outage, so the test is measuring a system that was
	// working rather than one that never did.
	if _, err := h.service.Purchase(ctx, testCampaign, "student-1", newKey()); err != nil {
		t.Fatalf("purchase before the outage = %v", err)
	}

	stopRedis(t, h)

	// Short budgets, or this test spends its life waiting for dials that were
	// never going to connect.
	h.service.timeouts = Timeouts{Redis: 2 * time.Second, Postgres: 2 * time.Second, Publish: time.Second}

	_, err := h.service.Purchase(ctx, testCampaign, "student-2", newKey())
	if err == nil {
		t.Fatal("a purchase succeeded with Redis gone — the stock invariant was not enforced by anything")
	}
	for _, wrong := range []error{domain.ErrSoldOut, domain.ErrAlreadyPurchased, domain.ErrCampaignNotFound} {
		if errors.Is(err, wrong) {
			t.Errorf("refusal reported as %v, which tells the caller the campaign decided something; it did not", wrong)
		}
	}

	// Nothing was written on the way down. A purchase that recorded a row
	// without a decrement would be worse than one that failed.
	live, err := h.store.CountLiveTickets(ctx, testCampaign)
	if err != nil {
		t.Fatalf("CountLiveTickets() = %v", err)
	}
	if live != 1 {
		t.Errorf("postgres holds %d tickets after a failed purchase, want the 1 from before the outage", live)
	}

	// Bring it back and check the campaign survived.
	//
	// The client is rebuilt rather than reused, and that is a limitation of
	// the harness rather than a statement about the system: Testcontainers
	// republishes on a new host port after a restart, so the old address now
	// points at nothing. In a deployment the address is stable and go-redis
	// reconnects on its own. What this can still prove — and what actually
	// matters — is that the *state* came back: the stock is where the outage
	// left it rather than reset to the campaign size, which is the failure
	// that would resell every ticket sold before the crash.
	recovered := restartRedis(t, h)

	remaining, _, err := recovered.Remaining(ctx, testCampaign)
	if err != nil {
		t.Fatalf("Remaining() after the restart = %v", err)
	}
	if remaining != 9 {
		t.Errorf("stock is %d after Redis came back, want the 9 the outage left — "+
			"anything higher resells tickets that are already sold", remaining)
	}

	held, err := recovered.HoldsTicket(ctx, testCampaign, "student-1")
	if err != nil {
		t.Fatalf("HoldsTicket() = %v", err)
	}
	if !held {
		t.Error("the buyer from before the outage is no longer a holder; they can buy a second ticket")
	}
}

// stopRedis takes the container away, which is a harder failure than a network
// blip: the process is gone and every connection with it.
func stopRedis(t *testing.T, h *harness) {
	t.Helper()

	timeout := 5 * time.Second
	if err := h.redisContainer.Stop(context.Background(), &timeout); err != nil {
		t.Fatalf("stopping redis: %v", err)
	}
}

// restartRedis brings the container back and returns a client pointed at it.
//
// A new client because the port moved, which Testcontainers does on every
// restart. Polled rather than slept: the container reports started before Redis
// is listening, and a fixed sleep is either flaky or slow.
func restartRedis(t *testing.T, h *harness) *cache.Cache {
	t.Helper()

	ctx := context.Background()
	if err := h.redisContainer.Start(ctx); err != nil {
		t.Fatalf("restarting redis: %v", err)
	}

	port, err := h.redisContainer.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatalf("reading the new redis port: %v", err)
	}
	host, err := h.redisContainer.Host(ctx)
	if err != nil {
		t.Fatalf("reading the redis host: %v", err)
	}

	recovered := openCache(t, net.JoinHostPort(host, port.Port()))

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		pingCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := recovered.Ping(pingCtx)
		cancel()
		if err == nil {
			return recovered
		}
		time.Sleep(250 * time.Millisecond)
	}

	t.Fatal("redis did not come back")
	return nil
}
