-- Replace the campaign's Redis state with what PostgreSQL already recorded.
--
-- The naive version of startup is `SET stock 100`, and it is a bug rather than
-- a simplification: every restart, deploy, crash and OOM kill refills the
-- shelf and sells tickets that are already in someone's hands. This script is
-- the alternative — the numbers are computed from the source of truth and
-- written over whatever Redis currently believes.
--
-- Rebuilding the buyer set matters as much as the counter and is easier to
-- forget, because nothing about the stock number looks wrong without it. A
-- process that restores 60 and loses the 40 people who already bought has a
-- correct shelf and no memory: those 40 can each take a second ticket, and the
-- fairness rule silently stops existing halfway through the campaign.
--
-- Both writes happen in one script so that the state is never observably
-- half-rebuilt. A DEL followed by a separate SADD leaves a window in which the
-- set is empty, and a purchase landing in that window is granted to someone who
-- already holds a ticket.
--
-- KEYS[1]     stock counter
-- KEYS[2]     set of user ids holding a ticket
-- ARGV[1]     remaining stock
-- ARGV[2..n]  the holders, if any
--
-- Returns the number of holders restored.

redis.call('SET', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])

-- SADD takes the members as arguments, and unpacking tens of thousands of them
-- at once overflows the Lua stack. Chunked, this scales with the campaign
-- rather than falling over at a size nobody tested.
local chunk = 1000
for i = 2, #ARGV, chunk do
    redis.call('SADD', KEYS[2], unpack(ARGV, i, math.min(i + chunk - 1, #ARGV)))
end

return #ARGV - 1
