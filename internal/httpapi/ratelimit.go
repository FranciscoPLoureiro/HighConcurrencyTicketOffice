package httpapi

import (
	"context"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"time"
)

// Limiter records one request against a budget and reports whether it fits,
// along with how long until it would.
//
// An interface so the transport layer does not depend on Redis, and so the
// middleware's own behaviour — the header, the status, the ordering of the two
// checks — can be tested without one.
type Limiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, time.Duration, error)
}

// Policy is one rate limit: how many requests, over how long.
type Policy struct {
	Limit  int
	Window time.Duration
}

// Enabled reports whether the policy restricts anything. A zero or negative
// limit turns the check off, which is what makes the limiter optional without
// every call site testing for nil.
func (p Policy) Enabled() bool { return p.Limit > 0 && p.Window > 0 }

// withRateLimit refuses callers who are asking too often.
//
// It runs after identity, not before. That ordering looks wrong for the per-IP
// limit — an unauthenticated flood skips it — and is right anyway: a request
// with no identity is rejected by the middleware above without touching Redis
// or PostgreSQL, so it is already the cheapest thing this service does. The
// traffic worth limiting is the traffic that gets as far as costing something,
// and the address check is here to catch one machine working through a list of
// invented user ids, which necessarily carries an identity.
func (s *Server) withRateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, _ := userIDFrom(r.Context())

		// Address first: it is the coarser of the two, so a single machine
		// cycling through identities is stopped before it can spend anyone
		// else's budget.
		if s.refuse(w, r, "ip", clientIP(r), s.ipLimit) {
			return
		}
		if s.refuse(w, r, "user", userID, s.userLimit) {
			return
		}

		next.ServeHTTP(w, r)
	})
}

// refuse applies one policy and reports whether it answered the request.
func (s *Server) refuse(w http.ResponseWriter, r *http.Request, scope, id string, policy Policy) bool {
	if s.limiter == nil || !policy.Enabled() || id == "" {
		return false
	}

	allowed, retryAfter, err := s.limiter.Allow(r.Context(), s.rateLimitKey(scope, id), policy.Limit, policy.Window)
	if err != nil {
		// Fail open, and only here.
		//
		// The stock invariant fails closed, because Redis is the only thing
		// that enforces it. A rate limit enforces no invariant: it protects
		// the system from load. If Redis is unreachable then the purchase
		// behind this middleware is going to fail anyway, and turning a
		// Redis blip into a blanket 429 for every caller would be an
		// outage invented on top of an outage.
		s.logger.Warn("rate limit check failed, allowing the request",
			slog.String("scope", scope),
			slog.Any("error", err))
		return false
	}

	if allowed {
		return false
	}

	// Retry-After is defined in whole seconds, and rounding down produces a
	// value that is still too early — a client that obeys it exactly comes
	// back to another 429. Rounded up, and never zero, so it always means
	// "later" and not "immediately".
	seconds := int(math.Ceil(retryAfter.Seconds()))
	seconds = max(seconds, 1)

	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	writeError(w, http.StatusTooManyRequests, "rate_limited",
		"too many requests; retry in "+strconv.Itoa(seconds)+"s")

	return true
}

// clientIP extracts the address to limit on.
//
// RemoteAddr and nothing else. X-Forwarded-For is trivially forged, and
// trusting it here would let a caller reset their own budget with every request
// by inventing a new address — a rate limiter that anyone can opt out of is
// worse than none, because it is believed. Behind a proxy this would have to
// read the header, from a hop count the proxy is configured to guarantee.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		// No port to strip, which happens in tests and with some
		// transports. The whole value is the best available answer.
		return r.RemoteAddr
	}
	return host
}
