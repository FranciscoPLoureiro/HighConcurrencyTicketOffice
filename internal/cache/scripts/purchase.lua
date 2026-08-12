-- Sell one ticket, or refuse it, as a single indivisible step.
--
-- This is the whole point of phase 2. Three facts have to be established and
-- one set of writes has to follow from them: this person does not already
-- hold a ticket, there is stock left, and therefore the stock drops by one and
-- the person is recorded as a holder. Split that across round trips and every
-- gap is a race — two requests both read stock = 1, or the same person's two
-- tabs both pass the duplicate check. Redis executes a script to completion
-- before it looks at another command, so there are no gaps to lose.
--
-- Since phase 5 it also records a *reservation*: the moment this ticket left
-- the shelf. The sale is not finished when this script returns — PostgreSQL
-- still has to record it and the broker still has to accept it — and a process
-- that dies in between would otherwise leave a ticket decremented from a stock
-- it never leaves, belonging to nobody, until somebody restarts the service.
-- The reservation is what lets a sweeper notice that later. See reservations.go.
--
-- KEYS[1]  stock counter for the campaign
-- KEYS[2]  set of user ids that already hold a ticket
-- KEYS[3]  sorted set of reservations, scored by when they were taken
-- ARGV[1]  the buyer
--
-- Returns { outcome, remaining }
--   0  sold          — the caller now owns a ticket and must record it
--   1  already held  — this person has one
--   2  sold out      — the campaign is empty
--   3  uninitialised — no stock key, so reconciliation has not run

local stock = redis.call('GET', KEYS[1])

-- Distinguished from "sold out" deliberately. A missing key means Redis has
-- been flushed or the campaign was never reconciled, and answering "no tickets
-- left" to that is a lie that looks like a normal, final refusal. The caller
-- turns this into a 404 and an alarming log line, not a 409.
if not stock then
    return { 3, 0 }
end

-- Checked before the stock, so that someone who already holds a ticket is told
-- so even after the campaign sells out. The alternative reports whichever
-- condition happened to be tested first, which means the answer to "why was I
-- refused?" changes depending on when you asked.
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
    return { 1, tonumber(stock) }
end

local remaining = tonumber(stock)
if remaining <= 0 then
    return { 2, 0 }
end

-- Redis reads its own clock rather than taking one from the caller.
--
-- The score is compared against a cutoff by a sweeper that may be running in a
-- different process on a different machine, and a reservation is released on
-- the strength of that comparison. Timestamps from several API instances would
-- be several clocks, and a machine running a few seconds fast would have its
-- reservations swept early — releasing a ticket whose sale is still in flight,
-- which is precisely the oversell this is built to avoid. One clock removes
-- the question.
local now = redis.call('TIME')
local millis = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)

redis.call('DECR', KEYS[1])
redis.call('SADD', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[3], millis, ARGV[1])

return { 0, remaining - 1 }
