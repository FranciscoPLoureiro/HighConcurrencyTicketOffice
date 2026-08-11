// Package cache owns access to Redis.
//
// From phase 2 onwards this is where the ticket stock invariant is enforced, so
// the package name understates its role: Redis is a cache in the sense that it
// can be rebuilt from Postgres, not in the sense that losing it is harmless.
//
// Every decision that has to be indivisible is written as a Lua script in
// scripts/ and executed there. The alternative, WATCH/MULTI/EXEC, is discussed
// in the README; the short version is that optimistic locking degrades as
// contention rises, and contention is the entire problem here.
package cache

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// The scripts are embedded rather than inlined as Go string literals so that
// they stay syntax-highlighted, diffable and lintable as Lua, and so a reader
// looking for the atomic step finds a file called purchase.lua.
var (
	//go:embed scripts/purchase.lua
	purchaseScriptSrc string
	//go:embed scripts/release.lua
	releaseScriptSrc string
)

// Cache is a handle on the Redis client and the scripts it runs.
type Cache struct {
	client *redis.Client

	// redis.Script sends EVALSHA first and only ships the body when the
	// server answers NOSCRIPT, so the script text crosses the wire once per
	// server lifetime instead of once per purchase. It also recovers by
	// itself from a SCRIPT FLUSH or a failover onto a replica that never saw
	// the script, which is the failure mode of caching the SHA by hand.
	purchase *redis.Script
	release  *redis.Script
}

// Open prepares a Redis client.
//
// As with the Postgres pool, connections are established lazily so that a brief
// outage leaves a process that can still explain itself over /health rather
// than one that refuses to start.
func Open(addr, password string) *Cache {
	return &Cache{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
		}),
		purchase: redis.NewScript(purchaseScriptSrc),
		release:  redis.NewScript(releaseScriptSrc),
	}
}

// Ping reports whether Redis is reachable and answering.
func (c *Cache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Close releases the underlying connections.
func (c *Cache) Close() error {
	return c.client.Close()
}

// The key layout. Braces are Redis Cluster hash tags: everything between them
// decides the slot, so a campaign's stock counter and its buyer set always
// live on the same node. A Lua script may only touch keys in one slot, and the
// purchase script touches both — without the tag it would work on a single
// instance and fail the first time anyone put it behind a cluster.
func stockKey(campaignID string) string  { return fmt.Sprintf("campaign:{%s}:stock", campaignID) }
func buyersKey(campaignID string) string { return fmt.Sprintf("campaign:{%s}:buyers", campaignID) }

// intPair reads the { code, value } shape every script in this package returns.
//
// Lua numbers come back as int64 through the RESP protocol, and a script that
// returns a table produces []any. Doing the assertion once keeps the type
// juggling out of the callers.
func intPair(v any, err error) (int64, int64, error) {
	if err != nil {
		return 0, 0, err
	}

	pair, ok := v.([]any)
	if !ok {
		return 0, 0, fmt.Errorf("script returned %T, want a pair", v)
	}
	if len(pair) != 2 {
		return 0, 0, fmt.Errorf("script returned %d values, want a pair", len(pair))
	}

	first, ok1 := pair[0].(int64)
	second, ok2 := pair[1].(int64)
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("script returned (%T, %T), want two integers", pair[0], pair[1])
	}

	return first, second, nil
}
