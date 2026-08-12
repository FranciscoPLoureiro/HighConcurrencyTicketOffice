-- Claim an idempotency key, or report what has already happened to it.
--
-- The check and the claim have to be one step for the same reason the purchase
-- does. A client whose request timed out sends it again, and the two attempts
-- can easily be in flight together — the first was never cancelled, it was just
-- slow. GET followed by SET leaves a gap in which both read "nothing here" and
-- both go on to buy a ticket, which is the phase 1 race wearing a different
-- hat.
--
-- The record holds a one-character state in front of its payload rather than
-- living in a hash. A hash would need HSETNX plus HGETALL and would still not
-- expire the two fields as one unit; a single string is claimed, read and
-- expired by one command each, and the prefix keeps "still working" and "here
-- is the answer" from ever being confused for one another.
--
-- KEYS[1]  the idempotency record
-- ARGV[1]  how long an unfinished claim may block a retry, in milliseconds
--
-- Returns { state, payload }
--   0  claimed   — the caller owns this key and must go and do the work
--   1  in flight — an earlier attempt is still working; payload is empty
--   2  replay    — the work finished; payload is the response it produced

local existing = redis.call('GET', KEYS[1])

if not existing then
    redis.call('SET', KEYS[1], 'P', 'PX', ARGV[1])
    return { 0, '' }
end

if string.sub(existing, 1, 1) == 'D' then
    return { 2, string.sub(existing, 2) }
end

return { 1, '' }
