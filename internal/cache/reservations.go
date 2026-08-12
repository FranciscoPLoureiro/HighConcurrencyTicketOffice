package cache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// reservationsKey names the sorted set holding open reservations.
//
// Hash-tagged like the campaign's other keys, because the purchase script
// touches it in the same breath as the stock counter and the buyer set, and a
// script may only touch keys in one slot.
func reservationsKey(campaignID string) string {
	return fmt.Sprintf("campaign:{%s}:reservations", campaignID)
}

// A sorted set scored by time, rather than one key per reservation with a TTL.
//
// The obvious design is a key per reservation and Redis's own expiry, with
// keyspace notifications telling somebody when one lapses. It does not work
// here, and the reason is worth stating because the mechanism looks purpose
// built for this.
//
// Keyspace notifications are fire and forget. Redis publishes the expiry event
// to whoever happens to be subscribed at that instant and keeps no record of
// it: a subscriber that is restarting, or briefly disconnected, or simply slow,
// never learns that the key expired — and neither does anyone else, ever. The
// event that goes missing is the one saying "this ticket was never sold, put it
// back", so the failure mode of the notification mechanism is exactly the
// failure it was chosen to fix, made permanent and silent.
//
// A sorted set is polled instead. Polling is less elegant and cannot lose
// anything: an entry stays until something removes it on purpose, so a sweeper
// that was down for an hour finds everything it missed on its first pass.

// Reservation is one ticket that left the shelf and has not been accounted for.
type Reservation struct {
	UserID string
	// Taken is when the Lua script granted the ticket, by Redis's clock.
	Taken time.Time
}

// Age reports how long the reservation has been open, measured against the
// same clock that scored it.
func (r Reservation) Age(now time.Time) time.Duration { return now.Sub(r.Taken) }

// Confirm closes a reservation, because the sale it was holding open is done.
func (c *Cache) Confirm(ctx context.Context, campaignID, userID string) (bool, error) {
	closed, err := c.confirm.Run(ctx, c.client,
		[]string{reservationsKey(campaignID)}, userID).Int64()
	if err != nil {
		return false, fmt.Errorf("run confirm script: %w", err)
	}
	return closed == 1, nil
}

// ExpiredReservations lists the reservations older than the given age.
//
// The cutoff is computed from Redis's clock rather than the caller's, for the
// same reason the score is: the two may be minutes apart, and a caller running
// fast would ask for everything older than a moment in the future.
//
// The limit bounds one pass. A sweeper that has been down while a campaign ran
// could otherwise pull every reservation in the campaign into memory and then
// issue a database query per entry, turning a recovery into an outage of its
// own. What it does not collect this pass, it collects on the next.
func (c *Cache) ExpiredReservations(ctx context.Context, campaignID string, olderThan time.Duration, limit int64) ([]Reservation, error) {
	now, err := c.client.Time(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("read redis clock: %w", err)
	}
	cutoff := now.Add(-olderThan).UnixMilli()

	entries, err := c.client.ZRangeByScoreWithScores(ctx, reservationsKey(campaignID), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(cutoff, 10),
		Count: limit,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("read expired reservations: %w", err)
	}

	reservations := make([]Reservation, 0, len(entries))
	for _, entry := range entries {
		userID, ok := entry.Member.(string)
		if !ok {
			return nil, fmt.Errorf("reservation member is %T, want a string", entry.Member)
		}
		reservations = append(reservations, Reservation{
			UserID: userID,
			Taken:  time.UnixMilli(int64(entry.Score)),
		})
	}

	return reservations, nil
}

// CountReservations reports how many reservations are open, which is what a
// test asserts and what a dashboard draws.
func (c *Cache) CountReservations(ctx context.Context, campaignID string) (int64, error) {
	open, err := c.client.ZCard(ctx, reservationsKey(campaignID)).Result()
	if err != nil {
		return 0, fmt.Errorf("count reservations: %w", err)
	}
	return open, nil
}
