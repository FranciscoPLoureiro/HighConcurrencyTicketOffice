//go:build integration

package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
)

// The claim has to be atomic, and only a real Redis can say whether it is.
//
// The unit tests for the middleware use a map behind a mutex, which proves that
// the middleware reacts correctly to each of the three answers and proves
// nothing at all about whether two processes can both be told "you have it".
// That question belongs to the Lua script, and this is where it is asked.
//
// The scenario is the one the mechanism exists for: a client whose request
// timed out sends it again while the original is still running. Both attempts
// are in flight together, and exactly one may be allowed to do the work.
func TestOnlyOneOfManySimultaneousAttemptsClaimsTheKey(t *testing.T) {
	ctx := context.Background()
	c := newTestCache(t)

	const attempts = 100
	key := IdempotencyKey(testCampaign, "student-1", "6f9619ff-8b86-d011-b42d-00cf4fc964ff")

	var claimed, inFlight, other atomic.Int64

	start := make(chan struct{})
	var wg sync.WaitGroup

	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			claim, _, err := c.ClaimIdempotency(ctx, key, time.Minute)
			if err != nil {
				t.Errorf("ClaimIdempotency() = %v", err)
				return
			}

			switch claim {
			case domain.ClaimAccepted:
				claimed.Add(1)
			case domain.ClaimInFlight:
				inFlight.Add(1)
			default:
				other.Add(1)
			}
		}()
	}

	close(start)
	wg.Wait()

	if claimed.Load() != 1 {
		t.Errorf("%d of %d simultaneous attempts claimed the key, want exactly 1",
			claimed.Load(), attempts)
	}
	if inFlight.Load() != attempts-1 {
		t.Errorf("%d attempts were told the key was in flight, want %d",
			inFlight.Load(), attempts-1)
	}
	if other.Load() != 0 {
		t.Errorf("%d attempts saw a replay of an answer nobody had produced", other.Load())
	}
}

// Claim, answer, claim again: the second caller gets the first one's response.
func TestAStoredResponseIsReplayedVerbatim(t *testing.T) {
	ctx := context.Background()
	c := newTestCache(t)

	key := IdempotencyKey(testCampaign, "student-1", "6f9619ff-8b86-d011-b42d-00cf4fc964ff")
	const answer = `{"status":202,"body":{"purchase_id":"abc"}}`

	claim, _, err := c.ClaimIdempotency(ctx, key, time.Minute)
	if err != nil || claim != domain.ClaimAccepted {
		t.Fatalf("first ClaimIdempotency() = %d, %v, want %d", claim, err, domain.ClaimAccepted)
	}

	if err := c.StoreResponse(ctx, key, answer, time.Hour); err != nil {
		t.Fatalf("StoreResponse() = %v", err)
	}

	claim, payload, err := c.ClaimIdempotency(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("second ClaimIdempotency() = %v", err)
	}
	if claim != domain.ClaimReplayed {
		t.Errorf("second claim = %d, want %d", claim, domain.ClaimReplayed)
	}
	if payload != answer {
		t.Errorf("replayed payload = %q, want %q", payload, answer)
	}
}

// A released key is free again, which is what lets a client retry a request
// that failed without producing an answer.
func TestAReleasedKeyCanBeClaimedAgain(t *testing.T) {
	ctx := context.Background()
	c := newTestCache(t)

	key := IdempotencyKey(testCampaign, "student-1", "6f9619ff-8b86-d011-b42d-00cf4fc964ff")

	if _, _, err := c.ClaimIdempotency(ctx, key, time.Minute); err != nil {
		t.Fatalf("ClaimIdempotency() = %v", err)
	}
	if err := c.ReleaseClaim(ctx, key); err != nil {
		t.Fatalf("ReleaseClaim() = %v", err)
	}

	claim, _, err := c.ClaimIdempotency(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("ClaimIdempotency() after release = %v", err)
	}
	if claim != domain.ClaimAccepted {
		t.Errorf("claim after release = %d, want %d — the caller cannot retry", claim, domain.ClaimAccepted)
	}
}

// An abandoned claim expires, so a process that died mid-request does not lock
// its caller out forever.
func TestAnExpiredLeaseFreesTheKey(t *testing.T) {
	ctx := context.Background()
	c := newTestCache(t)

	key := IdempotencyKey(testCampaign, "student-1", "6f9619ff-8b86-d011-b42d-00cf4fc964ff")

	if _, _, err := c.ClaimIdempotency(ctx, key, 50*time.Millisecond); err != nil {
		t.Fatalf("ClaimIdempotency() = %v", err)
	}

	// Slept rather than faked. The expiry is Redis's, and a clock this test
	// controls would only prove that Go can add two durations.
	time.Sleep(150 * time.Millisecond)

	claim, _, err := c.ClaimIdempotency(ctx, key, time.Minute)
	if err != nil {
		t.Fatalf("ClaimIdempotency() after the lease = %v", err)
	}
	if claim != domain.ClaimAccepted {
		t.Errorf("claim after the lease expired = %d, want %d", claim, domain.ClaimAccepted)
	}
}
