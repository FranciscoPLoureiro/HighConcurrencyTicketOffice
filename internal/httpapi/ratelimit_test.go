package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/health"
)

// stubLimiter answers however the test needs and records what it was asked.
type stubLimiter struct {
	allow      bool
	retryAfter time.Duration
	err        error
	keys       []string
}

func (s *stubLimiter) Allow(_ context.Context, key string, _ int, _ time.Duration) (bool, time.Duration, error) {
	s.keys = append(s.keys, key)
	if s.err != nil {
		return false, 0, s.err
	}
	return s.allow, s.retryAfter, nil
}

func limitedRoutes(limiter Limiter, user, ip Policy) http.Handler {
	return limitedRoutesFor(&stubPurchaser{}, limiter, user, ip)
}

func limitedRoutesFor(p Purchaser, limiter Limiter, user, ip Policy) http.Handler {
	return New(Config{
		Health:     health.New(),
		Purchaser:  p,
		CampaignID: testCampaign,
		Logger:     slog.New(slog.DiscardHandler),
		Limiter:    limiter,
		UserLimit:  user,
		IPLimit:    ip,
		// Scoped keys the assertions can read, standing in for the layout
		// the cache package actually uses.
		RateLimitKey: func(scope, id string) string { return scope + "|" + id },
	}).Routes()
}

var everyRequest = Policy{Limit: 1, Window: time.Minute}

func TestARateLimitedRequestIsRefusedWithRetryAfter(t *testing.T) {
	limiter := &stubLimiter{allow: false, retryAfter: 2500 * time.Millisecond}

	rec := postPurchase(t, limitedRoutes(limiter, everyRequest, Policy{}), "student-1")

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if got := decodeErrorCode(t, rec); got != "rate_limited" {
		t.Errorf("code = %q, want rate_limited", got)
	}

	// Rounded up: 2.5s truncated to 2 sends a well-behaved client back
	// half a second early, straight into another 429.
	retry := rec.Header().Get("Retry-After")
	if retry != "3" {
		t.Errorf("Retry-After = %q, want %q", retry, "3")
	}
}

// Retry-After: 0 tells a client to come back immediately, which is the one
// thing a rate limited client must not do.
func TestRetryAfterIsNeverZero(t *testing.T) {
	limiter := &stubLimiter{allow: false, retryAfter: 3 * time.Millisecond}

	rec := postPurchase(t, limitedRoutes(limiter, everyRequest, Policy{}), "student-1")

	seconds, err := strconv.Atoi(rec.Header().Get("Retry-After"))
	if err != nil {
		t.Fatalf("Retry-After = %q, want an integer", rec.Header().Get("Retry-After"))
	}
	if seconds < 1 {
		t.Errorf("Retry-After = %d, want at least 1", seconds)
	}
}

func TestAnAllowedRequestReachesTheHandler(t *testing.T) {
	limiter := &stubLimiter{allow: true}
	stub := &stubPurchaser{}

	rec := postPurchase(t, limitedRoutesFor(stub, limiter, everyRequest, Policy{}), "student-1")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if len(stub.calls) != 1 {
		t.Errorf("purchaser called %d times, want 1", len(stub.calls))
	}
}

// The address limit is the coarser of the two, so it is checked first: one
// machine working through invented identities is stopped before it can spend
// anybody else's budget.
func TestTheAddressIsCheckedBeforeTheAccount(t *testing.T) {
	limiter := &stubLimiter{allow: true}

	postPurchase(t, limitedRoutes(limiter, everyRequest, everyRequest), "student-1")

	if len(limiter.keys) != 2 {
		t.Fatalf("limiter consulted %d times, want 2 (address and account)", len(limiter.keys))
	}
	if limiter.keys[0][:3] != "ip|" {
		t.Errorf("first key checked was %q, want the address", limiter.keys[0])
	}
}

// A disabled policy must not cost a round trip to Redis on the hottest path in
// the system.
func TestADisabledPolicyIsNotConsulted(t *testing.T) {
	limiter := &stubLimiter{allow: true}

	postPurchase(t, limitedRoutes(limiter, Policy{}, Policy{}), "student-1")

	if len(limiter.keys) != 0 {
		t.Errorf("limiter was consulted %v with both policies disabled", limiter.keys)
	}
}

// The rate limiter fails open and the purchase fails closed. A limiter protects
// the system from load; it does not enforce an invariant, so turning a Redis
// blip into a blanket 429 invents an outage on top of an outage. The purchase
// behind it still refuses, because that one does guard the stock.
func TestTheLimiterFailsOpenWhenRedisIsUnreachable(t *testing.T) {
	limiter := &stubLimiter{err: errors.New("dial tcp: connection refused")}
	stub := &stubPurchaser{}

	rec := postPurchase(t, limitedRoutesFor(stub, limiter, everyRequest, Policy{}), "student-1")

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d: the limiter must not reject on its own failure", rec.Code, http.StatusOK)
	}
	if len(stub.calls) != 1 {
		t.Errorf("purchaser called %d times, want 1", len(stub.calls))
	}
}

func TestTheAccountLimitIsKeyedOnTheCaller(t *testing.T) {
	limiter := &stubLimiter{allow: true}

	postPurchase(t, limitedRoutes(limiter, everyRequest, Policy{}), "student-42")

	if len(limiter.keys) != 1 || limiter.keys[0] != "user|student-42" {
		t.Errorf("keys = %v, want [user|student-42]", limiter.keys)
	}
}

// Rate limiting is meaningless on a health check, and an orchestrator polling
// it must never be turned away.
func TestHealthIsNotRateLimited(t *testing.T) {
	limiter := &stubLimiter{allow: false}

	rec := httptest.NewRecorder()
	limitedRoutes(limiter, everyRequest, everyRequest).
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("health = %d, want %d", rec.Code, http.StatusOK)
	}
	if len(limiter.keys) != 0 {
		t.Errorf("the limiter was consulted for /health: %v", limiter.keys)
	}
}
