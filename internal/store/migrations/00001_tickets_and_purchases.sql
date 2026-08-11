-- +goose Up

-- One row per campaign holding its stock. A counter rather than a row per
-- physical ticket: nothing in this system distinguishes one ticket from
-- another, and 100 identical rows would only add a selection problem.
CREATE TABLE tickets (
    campaign_id TEXT        PRIMARY KEY,
    total       INTEGER     NOT NULL CHECK (total > 0),
    available   INTEGER     NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE purchases (
    id          UUID        PRIMARY KEY,
    campaign_id TEXT        NOT NULL REFERENCES tickets (campaign_id),
    user_id     TEXT        NOT NULL,
    status      TEXT        NOT NULL CHECK (status IN ('confirmed', 'cancelled')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX purchases_campaign_user_idx ON purchases (campaign_id, user_id);

-- Two constraints are conspicuously missing, and their absence is the point of
-- this migration rather than an oversight:
--
--   * no CHECK (available >= 0)
--   * no UNIQUE (campaign_id, user_id)
--
-- This phase exists to demonstrate that a SELECT followed by an UPDATE does not
-- hold under concurrency. Adding either constraint now would let the database
-- quietly enforce the invariant the application is failing to enforce, and the
-- demonstration would show nothing. Phase 2 moves the invariant to Redis and
-- adds the unique index as the backstop that must never be reachable.

-- +goose Down
DROP TABLE purchases;
DROP TABLE tickets;
