package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Allow records one request against a caller's budget and reports whether it
// fits.
//
// When it does not, the returned duration is how long until the oldest request
// in the window ages out and room appears — which is what belongs in a
// Retry-After header. A refused request is not recorded, so a client that
// ignores the header and keeps hammering does not push its own recovery further
// away with every attempt.
func (c *Cache) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, time.Duration, error) {
	allowed, retryMS, err := intPair(c.rateLimit.Run(ctx, c.client,
		[]string{key},
		time.Now().UnixMilli(),
		window.Milliseconds(),
		limit,
		// Unique per request: two requests landing in the same millisecond
		// would otherwise share a sorted set member and count once. Passed
		// in rather than generated in Lua so the script stays deterministic
		// and safe to replicate.
		uuid.NewString(),
	).Result())
	if err != nil {
		return false, 0, fmt.Errorf("run rate limit script: %w", err)
	}

	return allowed == 1, time.Duration(retryMS) * time.Millisecond, nil
}

// RateLimitKey names a caller's bucket within a scope, e.g. "user" or "ip".
func RateLimitKey(scope, id string) string { return fmt.Sprintf("ratelimit:%s:%s", scope, id) }
