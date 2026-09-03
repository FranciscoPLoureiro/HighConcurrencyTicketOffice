-- +goose Up

-- Phase 3 makes fulfilment asynchronous, so a purchase now has a life before it
-- is confirmed.
--
-- The API writes the row as 'pending' the instant Redis grants the ticket, and
-- the worker moves it to 'confirmed' once the document exists. That ordering is
-- a deliberate departure from the brief's diagram, which has the worker insert
-- the row instead; docs/DECISIONS.md argues it at length. The short version is
-- that startup reconciliation reads this table to decide how much stock is
-- left, so a sale the table will not hear about for two seconds is a sale that
-- a restart inside those two seconds hands out to somebody else.
--
-- 'failed' is a purchase the worker gave up on after exhausting its retries.
-- It is not 'cancelled', and the distance between the two is load bearing:
-- everything that asks whether a seat is taken asks `status <> 'cancelled'`, so
-- a failed purchase keeps its ticket off the shelf until something decides to
-- give it back. Returning it automatically would seat a second person while the
-- first still holds a row saying the seat is theirs.
ALTER TABLE purchases DROP CONSTRAINT purchases_status_check;
ALTER TABLE purchases ADD CONSTRAINT purchases_status_check
    CHECK (status IN ('pending', 'confirmed', 'failed', 'cancelled'));

-- The client's idempotency key, kept on the row it created.
--
-- The API answers a repeated request out of Redis, where the key and the
-- response it produced live under a TTL. This column is the durable half of the
-- same fact: it outlives the TTL, it survives a FLUSHALL, and it travels on
-- every queue message so that a redelivery can be tied back to the purchase it
-- refers to.
--
-- Unique per campaign rather than per user, which is one step stricter than the
-- API's own check. Redis scopes a claim to the caller, so nobody can replay
-- somebody else's response by guessing their key; this index says the stronger
-- thing, that a key names at most one purchase in the campaign at all. The two
-- are not in tension — one is about who may read a result, the other about how
-- many results there can be — and a UUID colliding between two honest clients
-- is not a case worth designing around.
--
-- Nullable, and the index partial to match: rows written before this migration
-- have no key, and inventing one for them would be a lie stored as data.
ALTER TABLE purchases ADD COLUMN idempotency_key UUID;

CREATE UNIQUE INDEX purchases_idempotency_key_idx
    ON purchases (campaign_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- When the row last changed state.
--
-- created_at answers "when did they buy?" and cannot also answer "how long has
-- this been pending?". The second question is the one an operator asks first
-- when the queue backs up, and the one phase 5.1's sweeper will ask to find
-- reservations that were never fulfilled.
ALTER TABLE purchases ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- +goose Down

DROP INDEX purchases_idempotency_key_idx;
ALTER TABLE purchases DROP COLUMN idempotency_key;
ALTER TABLE purchases DROP COLUMN updated_at;

-- This fails if any row is still 'pending' or 'failed', and that is the correct
-- behaviour rather than an oversight. The alternative is to rewrite those rows
-- into a status they were never in, which would silently confirm tickets that
-- were never fulfilled and abandon ones that were only waiting.
ALTER TABLE purchases DROP CONSTRAINT purchases_status_check;
ALTER TABLE purchases ADD CONSTRAINT purchases_status_check
    CHECK (status IN ('confirmed', 'cancelled'));
