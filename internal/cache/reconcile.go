package cache

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// Reconcile overwrites the campaign's Redis state with the given truth.
//
// Callers are expected to have read `remaining` and `buyers` from PostgreSQL
// while holding the reconciliation lock — see WithLock. Nothing here validates
// that, because nothing here can.
func (c *Cache) Reconcile(ctx context.Context, campaignID string, remaining int, buyers []string) error {
	args := make([]any, 0, len(buyers)+1)
	args = append(args, remaining)
	for _, buyer := range buyers {
		args = append(args, buyer)
	}

	if err := c.reconcile.Run(ctx, c.client,
		[]string{stockKey(campaignID), buyersKey(campaignID), reservationsKey(campaignID)},
		args...,
	).Err(); err != nil {
		return fmt.Errorf("run reconcile script: %w", err)
	}

	return nil
}

// Remaining reports the stock Redis currently believes in.
//
// Reported as a count and a flag rather than as a count and an error, because
// "no such key" is a normal answer here — it is what an unreconciled campaign
// looks like — and callers should not have to tell it apart from a failure.
func (c *Cache) Remaining(ctx context.Context, campaignID string) (int64, bool, error) {
	remaining, err := c.client.Get(ctx, stockKey(campaignID)).Int64()
	switch {
	case err == nil:
		return remaining, true, nil
	case errors.Is(err, redis.Nil):
		return 0, false, nil
	default:
		return 0, false, fmt.Errorf("read remaining stock: %w", err)
	}
}

// HoldsTicket reports whether Redis has this person down as a ticket holder.
func (c *Cache) HoldsTicket(ctx context.Context, campaignID, userID string) (bool, error) {
	held, err := c.client.SIsMember(ctx, buyersKey(campaignID), userID).Result()
	if err != nil {
		return false, fmt.Errorf("read ticket holder: %w", err)
	}
	return held, nil
}

// CountHolders reports how many people Redis has down as ticket holders.
func (c *Cache) CountHolders(ctx context.Context, campaignID string) (int64, error) {
	holders, err := c.client.SCard(ctx, buyersKey(campaignID)).Result()
	if err != nil {
		return 0, fmt.Errorf("count ticket holders: %w", err)
	}
	return holders, nil
}

// SetRemaining writes the stock counter without consulting anything.
//
// This is the bug the reconciliation exists to prevent, and it is exported so
// that a test can commit it deliberately: put a wrong number in Redis, restart,
// and assert that startup replaced it with the truth. Nothing on the serving
// path calls this.
func (c *Cache) SetRemaining(ctx context.Context, campaignID string, remaining int) error {
	if err := c.client.Set(ctx, stockKey(campaignID), strconv.Itoa(remaining), 0).Err(); err != nil {
		return fmt.Errorf("set remaining stock: %w", err)
	}
	return nil
}
