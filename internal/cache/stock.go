package cache

import (
	"context"
	"fmt"
)

// Outcome is what the purchase script decided.
type Outcome int64

// The outcomes, matching the codes returned by scripts/purchase.lua.
const (
	// Sold means the caller now owns a ticket and has to record it.
	Sold Outcome = 0
	// AlreadyHeld means this person already holds one.
	AlreadyHeld Outcome = 1
	// SoldOut means the campaign has no stock left.
	SoldOut Outcome = 2
	// Uninitialised means Redis holds no stock counter for this campaign,
	// so reconciliation has not run. Distinct from SoldOut on purpose: one
	// is the campaign working as intended and the other is the system not
	// knowing anything, and answering "sold out" to the second is a
	// convincing lie.
	Uninitialised Outcome = 3
)

// Purchase attempts to take one ticket for one user, atomically.
//
// The check for an existing ticket, the check for stock, the decrement and the
// record of who took it all happen inside one script, so no other request can
// observe or interleave with a partial result. It returns what was decided and
// how much stock is left after it.
func (c *Cache) Purchase(ctx context.Context, campaignID, userID string) (Outcome, int64, error) {
	outcome, remaining, err := intPair(c.purchase.Run(ctx, c.client,
		[]string{stockKey(campaignID), buyersKey(campaignID), reservationsKey(campaignID)},
		userID,
	).Result())
	if err != nil {
		return 0, 0, fmt.Errorf("run purchase script: %w", err)
	}

	return Outcome(outcome), remaining, nil
}

// Release gives a ticket back and lets its buyer try again.
//
// The undo for a purchase that Redis granted and the rest of the system then
// failed to complete. It is idempotent: running it twice returns one ticket,
// not two. That is not a nicety — a compensation that increments the stock
// every time it runs is how a hundred tickets becomes a hundred and one, and
// the callers of this are all on retry paths.
//
// It reports whether it actually released anything, so a caller can tell "I
// undid the purchase" from "there was nothing to undo".
func (c *Cache) Release(ctx context.Context, campaignID, userID string) (bool, error) {
	remaining, err := c.release.Run(ctx, c.client,
		[]string{stockKey(campaignID), buyersKey(campaignID), reservationsKey(campaignID)},
		userID,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("run release script: %w", err)
	}

	return remaining >= 0, nil
}
