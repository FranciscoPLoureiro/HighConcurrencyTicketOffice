-- +goose Up

-- The idempotency index now ignores cancelled rows, exactly as the fairness
-- index already does.
--
-- Migration 00003 made the index cover every row that has a key, on the
-- reasoning that a key should name at most one purchase in the campaign at
-- all. That reads well and is wrong once a sale can be reversed, because it
-- makes a cancelled purchase keep its key forever — and the key is the one
-- thing the client is told to send again.
--
-- The sequence that breaks it is the documented retry protocol, followed
-- exactly. A purchase is recorded, the broker refuses the publish, the sale is
-- reversed: the row is cancelled, the ticket goes back on the shelf, the buyer
-- leaves the buyer set, and the caller is answered `500`. A `5xx` is not an
-- answer, so the API releases the Idempotency-Key so the caller can retry with
-- it — that is the whole point of releasing it. The retry then passes the Lua
-- script, because the buyer really is free to buy again, and dies here on the
-- cancelled row's key. The caller is told `idempotency_key_replayed`: this key
-- already produced a purchase and the response is no longer available. Both
-- halves of that sentence are false. There is no purchase, and no retry with
-- that key will ever succeed, so an honest client doing what the API asked of
-- it can never buy a ticket.
--
-- The same hole opens after the compensation saga, which reverses a sale the
-- worker gave up on and is required to leave the buyer able to buy again.
--
-- Scoping the index to live rows keeps everything it was protecting. A key
-- still cannot produce two live purchases, which is the property that stops a
-- replay taking a second ticket; what it can now do is name one cancelled
-- attempt and the successful retry that followed it, which is the history
-- actually worth having. It also puts this index and the fairness index on the
-- same rule, so "cancelled means it no longer holds anything" is true of both
-- rather than of one.
DROP INDEX purchases_idempotency_key_idx;

CREATE UNIQUE INDEX purchases_idempotency_key_idx
    ON purchases (campaign_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL AND status <> 'cancelled';

-- +goose Down

-- This fails if any key is held by both a cancelled row and a live one, which
-- is precisely the state the Up allows and the old index forbade. Failing is
-- correct: the alternative is to delete somebody's purchase history to make an
-- index fit.
DROP INDEX purchases_idempotency_key_idx;

CREATE UNIQUE INDEX purchases_idempotency_key_idx
    ON purchases (campaign_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
