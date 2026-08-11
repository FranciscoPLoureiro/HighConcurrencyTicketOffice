-- +goose Up

-- The backstop for the fairness rule, now that Redis enforces it.
--
-- From phase 2 the one-ticket-per-user check lives in a Lua script, where it
-- shares an atomic step with the stock check. This index does not implement
-- that rule; it makes it impossible for the database to disagree with it. If a
-- second confirmed row for the same person ever reaches here, something has
-- gone wrong upstream — Redis was flushed, reconciliation ran against the
-- wrong campaign, a bug slipped past the script — and the write must fail
-- loudly instead of quietly becoming the second ticket someone was sold.
--
-- The partial WHERE is the load-bearing part. A plain UNIQUE (campaign_id,
-- user_id) would also stop the compensation saga in phase 5.3 from ever
-- working: cancelling a purchase has to release the buyer to try again, and it
-- does that by moving the row to 'cancelled' rather than deleting it, so the
-- history of what happened survives. Excluding cancelled rows from the index
-- is what lets the same person hold at most one *live* ticket while leaving
-- any number of reversed attempts behind them.
--
-- Not created CONCURRENTLY: goose runs each migration inside a transaction and
-- CREATE INDEX CONCURRENTLY cannot run in one. On a table this size the
-- exclusive lock is measured in milliseconds. On a large table it would not
-- be, and this would need to move outside the migration.
CREATE UNIQUE INDEX purchases_one_live_ticket_per_user_idx
    ON purchases (campaign_id, user_id)
    WHERE status <> 'cancelled';

-- The existing (campaign_id, user_id) index is now redundant: the unique index
-- above covers the same columns in the same order and serves every lookup the
-- old one did.
DROP INDEX purchases_campaign_user_idx;

-- +goose Down
CREATE INDEX purchases_campaign_user_idx ON purchases (campaign_id, user_id);
DROP INDEX purchases_one_live_ticket_per_user_idx;
