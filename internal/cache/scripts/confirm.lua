-- Close a reservation, because the sale it was holding open has completed.
--
-- Called once the purchase is recorded in PostgreSQL and the broker has taken
-- responsibility for fulfilling it. From this point the ticket is accounted for
-- in the source of truth, and the sweeper has no business releasing it.
--
-- Failing to run this is survivable, which is the reason it is a separate step
-- rather than part of the publish. A reservation left open is found by the
-- sweeper, checked against PostgreSQL, seen to belong to a real purchase, and
-- closed then instead. The cost is one wasted database read; the alternative —
-- folding the close into an earlier step — would mean closing the reservation
-- before the thing it protects against can no longer happen.
--
-- KEYS[1]  sorted set of reservations
-- ARGV[1]  the buyer
--
-- Returns 1 if a reservation was closed, 0 if there was nothing to close.

return redis.call('ZREM', KEYS[1], ARGV[1])
