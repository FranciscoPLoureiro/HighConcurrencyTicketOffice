-- Release a lock only if we are still the holder.
--
-- A plain DEL is wrong, and wrong in a way that only shows up under load. If
-- the holder stalls past the lock's expiry, Redis hands the lock to someone
-- else; the first process then wakes up and deletes a lock it no longer owns,
-- and now two processes are inside the critical section believing they are
-- alone. Comparing the token first makes the release a no-op in that case, and
-- the comparison has to be atomic with the delete or the same race reappears
-- one instruction later.
--
-- KEYS[1]  the lock key
-- ARGV[1]  the token this process wrote when it acquired the lock
--
-- Returns 1 if the lock was released by us, 0 if it had already moved on.

if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end

return 0
