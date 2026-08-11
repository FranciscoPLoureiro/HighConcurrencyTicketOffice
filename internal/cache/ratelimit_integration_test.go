//go:build integration

// Integration tests for the Redis-backed pieces, against a real Redis started
// by Testcontainers.
//
// The rate limiter is a Lua script over a sorted set, and the properties worth
// testing — that a window actually slides, that concurrent callers cannot
// exceed the limit between them — are properties of Redis executing that
// script, not of the Go around it.
package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

func newTestCache(t *testing.T) *Cache {
	t.Helper()

	ctx := context.Background()

	container, err := tcredis.Run(ctx, "redis:8-alpine")
	if err != nil {
		t.Fatalf("starting redis: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminating redis: %v", err)
		}
	})

	endpoint, err := container.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("reading redis endpoint: %v", err)
	}

	c := Open(endpoint, "")
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Logf("closing redis client: %v", err)
		}
	})

	return c
}

func TestTheLimitIsTheLimit(t *testing.T) {
	ctx := context.Background()
	c := newTestCache(t)

	const limit = 5
	key := RateLimitKey("user", "student-1")

	for i := 1; i <= limit; i++ {
		allowed, _, err := c.Allow(ctx, key, limit, time.Minute)
		if err != nil {
			t.Fatalf("Allow() %d = %v", i, err)
		}
		if !allowed {
			t.Fatalf("request %d of %d was refused", i, limit)
		}
	}

	allowed, retryAfter, err := c.Allow(ctx, key, limit, time.Minute)
	if err != nil {
		t.Fatalf("Allow() = %v", err)
	}
	if allowed {
		t.Error("request 6 of 5 was allowed")
	}
	// A Retry-After of zero tells a client to come back immediately, which
	// is the one thing a rate limited client must not do.
	if retryAfter <= 0 {
		t.Errorf("retryAfter = %s, want a positive duration", retryAfter)
	}
	if retryAfter > time.Minute {
		t.Errorf("retryAfter = %s, want no more than the window", retryAfter)
	}
}

// Budgets belong to callers, not to the service.
func TestCallersDoNotShareABudget(t *testing.T) {
	ctx := context.Background()
	c := newTestCache(t)

	if allowed, _, err := c.Allow(ctx, RateLimitKey("user", "a"), 1, time.Minute); err != nil || !allowed {
		t.Fatalf("first caller = %t, %v", allowed, err)
	}
	if allowed, _, err := c.Allow(ctx, RateLimitKey("user", "b"), 1, time.Minute); err != nil || !allowed {
		t.Errorf("second caller = %t, %v; one caller's budget was spent by another", allowed, err)
	}
}

// The reason for a sliding window rather than a fixed one. Room reappears as
// individual requests age out, not in a lump when a clock boundary passes.
func TestTheWindowSlides(t *testing.T) {
	ctx := context.Background()
	c := newTestCache(t)

	const window = 700 * time.Millisecond
	key := RateLimitKey("user", "student-1")

	if allowed, _, err := c.Allow(ctx, key, 1, window); err != nil || !allowed {
		t.Fatalf("first request = %t, %v", allowed, err)
	}
	if allowed, _, err := c.Allow(ctx, key, 1, window); err != nil || allowed {
		t.Fatalf("second request = %t, %v, want refused", allowed, err)
	}

	// A generous margin past the window. Waiting exactly one window would
	// make the result depend on scheduling noise rather than on whether the
	// window slides.
	time.Sleep(window + 300*time.Millisecond)

	allowed, _, err := c.Allow(ctx, key, 1, window)
	if err != nil {
		t.Fatalf("Allow() after the window = %v", err)
	}
	if !allowed {
		t.Error("the request was still refused after its window had passed")
	}
}

// A refused request must not be recorded, or a client that ignores Retry-After
// and keeps hammering pushes its own recovery further away with every attempt
// and never gets back in.
func TestRefusedRequestsDoNotExtendTheBan(t *testing.T) {
	ctx := context.Background()
	c := newTestCache(t)

	const (
		window = time.Second
		// The hammering stops well short of the window closing, and the
		// wait runs well past it. Timing either boundary tightly makes the
		// test flake on the machine rather than on the behaviour: a request
		// issued a millisecond after the window expires is *supposed* to be
		// allowed, and would be read here as a failure.
		hammerUntil = 600 * time.Millisecond
		waitUntil   = window + 300*time.Millisecond
	)
	key := RateLimitKey("user", "impatient")

	admitted := time.Now()
	if allowed, _, err := c.Allow(ctx, key, 1, window); err != nil || !allowed {
		t.Fatalf("first request = %t, %v", allowed, err)
	}

	for time.Since(admitted) < hammerUntil {
		allowed, _, err := c.Allow(ctx, key, 1, window)
		if err != nil {
			t.Fatalf("Allow() = %v", err)
		}
		if allowed {
			t.Fatalf("a second request was allowed %s into a %s window", time.Since(admitted), window)
		}
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(waitUntil - time.Since(admitted))

	allowed, _, err := c.Allow(ctx, key, 1, window)
	if err != nil {
		t.Fatalf("Allow() = %v", err)
	}
	if !allowed {
		t.Error("hammering during the window pushed the recovery out of reach")
	}
}

// Two requests in the same millisecond must count as two. They share a sorted
// set score, so without a unique member per request the second overwrites the
// first and the limit is quietly larger than configured.
func TestSimultaneousRequestsAreCountedIndividually(t *testing.T) {
	ctx := context.Background()
	c := newTestCache(t)

	const (
		limit    = 20
		attempts = 200
	)
	key := RateLimitKey("ip", "203.0.113.7")

	var allowedCount atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup

	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			allowed, _, err := c.Allow(ctx, key, limit, time.Minute)
			if err != nil {
				t.Errorf("Allow() = %v", err)
				return
			}
			if allowed {
				allowedCount.Add(1)
			}
		}()
	}

	close(start)
	wg.Wait()

	if allowedCount.Load() != limit {
		t.Errorf("%d of %d simultaneous requests were allowed, want exactly %d",
			allowedCount.Load(), attempts, limit)
	}
}
