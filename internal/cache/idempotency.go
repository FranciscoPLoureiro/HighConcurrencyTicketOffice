package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
)

// claimDone is the one-character prefix marking a record that holds an answer,
// as opposed to the "P" the script writes when it claims a key. The state sits
// in front of the payload rather than in a second key so that the two expire
// together and can never be observed apart.
const claimDone = "D"

// IdempotencyKey names the record for one caller's use of one key.
//
// Scoped to the user, not just to the campaign. The key is a value the client
// chooses, so without the user in the name anybody who learned somebody else's
// key could ask for the response it produced — which is a stranger's purchase
// id and a stranger's ticket. The database holds the stricter rule that a key
// names at most one purchase anywhere in the campaign; this one is about who is
// allowed to read the answer.
//
// Deliberately not hash-tagged, unlike the campaign's stock and buyer set. The
// tag exists so a Lua script can touch two keys in one slot, and nothing here
// touches two. Tagging anyway would pin every idempotency record in the
// campaign to a single node, which is the one shape of key that should be
// spread as widely as possible.
func IdempotencyKey(campaignID, userID, key string) string {
	return fmt.Sprintf("campaign:%s:idem:%s:%s", campaignID, userID, key)
}

// ClaimIdempotency takes ownership of a key, or reports what became of it.
//
// The lease bounds how long an attempt that never finished may block a retry.
// It wants to be comfortably longer than a request can take and much shorter
// than a person's patience: too short and a slow first attempt lets a retry
// through to buy a second ticket, too long and a caller whose request died with
// the process is locked out for no reason.
func (c *Cache) ClaimIdempotency(ctx context.Context, key string, lease time.Duration) (domain.Claim, string, error) {
	result, err := c.idempotency.Run(ctx, c.client, []string{key}, lease.Milliseconds()).Result()
	if err != nil {
		return 0, "", fmt.Errorf("run idempotency script: %w", err)
	}

	pair, ok := result.([]any)
	if !ok || len(pair) != 2 {
		return 0, "", fmt.Errorf("idempotency script returned %T, want a pair", result)
	}

	state, ok := pair[0].(int64)
	if !ok {
		return 0, "", fmt.Errorf("idempotency script returned a %T state, want an integer", pair[0])
	}
	// An empty payload comes back as an empty bulk string, which go-redis
	// decodes as a string; anything else is a protocol surprise worth naming.
	payload, ok := pair[1].(string)
	if !ok {
		return 0, "", fmt.Errorf("idempotency script returned a %T payload, want a string", pair[1])
	}

	return domain.Claim(state), payload, nil
}

// StoreResponse records the answer a claimed key produced.
//
// The retention is how long a retry can still be recognised as one. It is not a
// correctness knob — the stock invariant does not depend on it — but on how
// long after giving up a client may reasonably come back. Past it, a retry gets
// the ordinary refusal for someone who already holds a ticket, which is true
// but less useful than the original answer.
//
// A plain SET rather than a compare-and-set against our own claim. The only way
// to overwrite somebody else's record is for our lease to have expired while we
// were still working, and in that case the record we are writing describes the
// request that actually got the ticket.
func (c *Cache) StoreResponse(ctx context.Context, key, response string, retention time.Duration) error {
	if err := c.client.Set(ctx, key, claimDone+response, retention).Err(); err != nil {
		return fmt.Errorf("store idempotent response: %w", err)
	}
	return nil
}

// ReleaseClaim gives a key back so that a retry may use it.
//
// For failures that produced no answer at all. A refused purchase is an answer
// and gets stored; a request that fell over on the way to deciding anything is
// not, and holding its key would leave the caller unable to retry with the only
// key that could have been recognised.
func (c *Cache) ReleaseClaim(ctx context.Context, key string) error {
	if err := c.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("release idempotency claim: %w", err)
	}
	return nil
}
