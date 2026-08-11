package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ErrLockUnavailable means the lock was held by someone else for the whole time
// the caller was prepared to wait.
var ErrLockUnavailable = errors.New("lock is held elsewhere")

// lockPollInterval is how often a waiter re-checks a held lock. Short enough
// that a rolling deploy does not visibly stall behind it, long enough that a
// dozen instances waiting together do not turn into a busy loop against Redis.
const lockPollInterval = 100 * time.Millisecond

// WithLock runs fn while holding a mutually exclusive lock on key.
//
// It blocks until the lock is granted, ctx ends, or wait elapses. The lock
// carries a TTL so that a holder which dies without releasing does not block
// every other instance forever — the cost of crashing is a delay, not a stuck
// deployment.
//
// This is not Redlock, and on a single Redis instance it cannot be: if that
// instance fails over to a replica that has not yet received the SET, two
// holders can exist at once. It is used here for a critical section whose work
// is idempotent — recomputing the same numbers from the same source of truth —
// so the failure mode is duplicated effort rather than a violated invariant.
// A lock protecting something that is not idempotent would need more than this.
func (c *Cache) WithLock(ctx context.Context, key string, ttl, wait time.Duration, fn func(context.Context) error) error {
	// A token unique to this acquisition. Without it, release is a plain DEL
	// and a slow holder whose lock has already expired deletes the lock now
	// belonging to somebody else. See scripts/unlock.lua.
	token := uuid.NewString()

	acquired, err := c.acquire(ctx, key, token, ttl, wait)
	if err != nil {
		return err
	}
	if !acquired {
		return fmt.Errorf("%w after %s: %s", ErrLockUnavailable, wait, key)
	}

	defer func() {
		// WithoutCancel: releasing has to happen even when fn failed
		// because the context expired. Skipping it would leave the key to
		// its TTL and stall the next instance for no reason.
		//
		// The error is dropped rather than returned. If the release fails
		// the TTL still expires the lock, so the consequence is a delay
		// rather than a deadlock — and the caller's own error, when there
		// is one, is the one worth propagating.
		_ = c.unlock.Run(context.WithoutCancel(ctx), c.client, []string{key}, token).Err()
	}()

	return fn(ctx)
}

// acquire polls for the lock until it is granted or the caller runs out of
// patience.
func (c *Cache) acquire(ctx context.Context, key, token string, ttl, wait time.Duration) (bool, error) {
	deadline := time.NewTimer(wait)
	defer deadline.Stop()

	ticker := time.NewTicker(lockPollInterval)
	defer ticker.Stop()

	for {
		// SET NX PX is the whole acquisition: it writes the token only if
		// the key is absent, and attaches the expiry in the same command,
		// so there is no window in which the lock exists without a TTL.
		ok, err := c.client.SetNX(ctx, key, token, ttl).Result()
		if err != nil {
			return false, fmt.Errorf("acquire lock %q: %w", key, err)
		}
		if ok {
			return true, nil
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-deadline.C:
			return false, nil
		case <-ticker.C:
		}
	}
}

// ReconcileLockKey names the lock that serialises startup reconciliation for a
// campaign. Exported so that the caller doing the reconciliation — which lives
// outside this package, because it has to read PostgreSQL — can name the same
// lock without inventing its own key.
//
// Hash-tagged like the campaign's other keys, so that a cluster keeps the lock
// on the same node as the state it protects.
func ReconcileLockKey(campaignID string) string {
	return fmt.Sprintf("campaign:{%s}:lock:reconcile", campaignID)
}
