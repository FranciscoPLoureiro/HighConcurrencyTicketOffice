// Package cache owns access to Redis.
//
// From phase 2 onwards this is where the ticket stock invariant is enforced, so
// the package name understates its role: Redis is a cache in the sense that it
// can be rebuilt from Postgres, not in the sense that losing it is harmless.
package cache

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Cache is a handle on the Redis client.
type Cache struct {
	client *redis.Client
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
