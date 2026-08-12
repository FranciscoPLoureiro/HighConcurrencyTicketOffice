-- Sliding window rate limit over a sorted set of request timestamps.
--
-- A fixed window counter is cheaper — one INCR and an expiry — but it lets a
-- caller spend a full window's allowance in the last moment of one window and
-- the whole of the next allowance immediately after, so the real limit is twice
-- the configured one across the boundary. On a campaign that is decided in the
-- first few seconds, that boundary is the only part of the timeline that
-- matters, so the cheap version is cheap in exactly the wrong place.
--
-- The cost is memory proportional to the limit, per caller, which is why the
-- key expires with the window rather than lingering after the campaign.
--
-- KEYS[1]  the caller's bucket
-- ARGV[1]  window length, in milliseconds
-- ARGV[2]  how many requests the window allows
-- ARGV[3]  a unique member for this request
--
-- Returns { allowed, retry_after_ms }

local window = tonumber(ARGV[1])
local limit  = tonumber(ARGV[2])

-- Redis reads its own clock rather than taking one from the caller, for the
-- same reason purchase.lua does: several API instances are several clocks, and
-- every score written here is compared against a cutoff computed by whichever
-- instance happens to serve the next request.
--
-- The consequence of mixing them is not a slightly wrong window. An instance
-- running a minute fast writes scores a minute in the future; on an instance
-- with the correct time those entries never age out of the window, so the
-- bucket stays full and that caller is refused everything until the skew
-- passes. The retry below then computes a negative wait, clamps it to zero,
-- and the caller is told to come straight back into another refusal. One clock
-- removes the question.
local clock = redis.call('TIME')
local now = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)

-- Everything that has aged out of the window stops counting. Doing this first
-- means the ZCARD below is the live count and not a running total.
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)

if redis.call('ZCARD', KEYS[1]) >= limit then
    -- Room appears when the oldest surviving request leaves the window, so
    -- that is when it is worth coming back. Telling the caller the window
    -- length instead would be correct but pessimistic, and Retry-After is
    -- only useful to a well-behaved client if it is close to true.
    local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
    local retry = window - (now - tonumber(oldest[2]))
    if retry < 0 then
        retry = 0
    end
    return { 0, retry }
end

-- The member has to be unique per request or two requests in the same
-- millisecond collapse into one ZSET entry and the second is never counted.
--
-- It is supplied by the caller because it cannot be generated here. Redis
-- seeds the Lua PRNG identically for every invocation, so math.random returns
-- the same sequence to every run of this script — a member built from it would
-- be the same member every time, which is the collision this argument exists
-- to prevent, made total.
redis.call('ZADD', KEYS[1], now, ARGV[3])
redis.call('PEXPIRE', KEYS[1], window)

return { 1, 0 }
