-- Put a ticket back and let its buyer try again.
--
-- The undo for a purchase that Redis granted and the rest of the system then
-- failed to complete. It has to be idempotent, because everything that calls it
-- is already on a path where something went wrong once and may well go wrong
-- twice: a retry, a reconciliation job and a compensation consumer can all
-- reach for the same ticket.
--
-- SREM reports whether it actually removed anything, and the INCR is
-- conditional on that. Run twice, the second call removes nothing and stops, so
-- the stock goes up by one rather than by two. Written as two separate commands
-- this is exactly the bug the brief warns about — "if the compensation runs
-- twice, the stock increments twice and you now have 101 tickets" — and the
-- reason it is not that bug here is that the test and the increment are one
-- step.
--
-- KEYS[1]  stock counter
-- KEYS[2]  set of user ids holding a ticket
-- ARGV[1]  the buyer to release
--
-- Returns the new stock, or -1 if this person held no ticket.

if redis.call('SREM', KEYS[2], ARGV[1]) == 0 then
    return -1
end

return redis.call('INCR', KEYS[1])
