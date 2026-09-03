# The API

A purchase needs two headers: who is buying, and a key that makes the request
safe to send twice.

```bash
curl -X POST localhost:8080/api/v1/tickets/purchase \
  -H 'X-User-ID: student-1' \
  -H "Idempotency-Key: $(uuidgen)"
```

```json
{
  "purchase_id": "5a3f1c88-2e4b-4f7a-9d21-0c6b8e1a4f30",
  "campaign_id": "queima-2026",
  "user_id": "student-1",
  "status": "pending",
  "correlation_id": "c0ffee00-1111-4222-8333-444455556666",
  "status_url": "/api/v1/tickets/5a3f1c88-2e4b-4f7a-9d21-0c6b8e1a4f30/status",
  "created_at": "2026-08-12T00:00:00.123456Z"
}
```

**`202`, not `200`.** The seat is decided and durably recorded; the document is
not, and will not be for another two seconds. Answering `200` would promise a
ticket that does not exist yet. The caller polls `status_url` — or, if it has
one, watches for the `correlation_id` in the logs.

```bash
curl localhost:8080/api/v1/tickets/{id}/status -H 'X-User-ID: student-1'
```

```json
{ "purchase_id": "5a3f…", "status": "confirmed",
  "created_at": "…", "updated_at": "…" }
```

A purchase moves `pending → confirmed` when the worker finishes it,
`pending → failed` if it exhausts its retries, and `→ cancelled` only if
something deliberately gives the ticket back. Everything except `cancelled`
holds a seat.

Every outcome has its own status and its own machine-readable code. Several
share a status, so the code is the contract and the prose beside it is not —
a client should switch on `error.code`, never on the message.

| Status | `error.code` | Means |
|---|---|---|
| `202` | — | The seat is yours; the ticket is being generated |
| `409` | `stock_exhausted` | The campaign is empty |
| `409` | `already_purchased` | You already hold one |
| `409` | `idempotency_key_in_use` | An earlier request with this key is still running |
| `409` | `idempotency_key_replayed` | This key already bought a ticket, and the response is no longer cached |
| `429` | `rate_limited` | Too many attempts; see `Retry-After` |
| `400` | `missing_idempotency_key` | No `Idempotency-Key` header |
| `400` | `invalid_idempotency_key` | The key is not a UUID |
| `400` | `invalid_identity` | The `X-User-ID` header is longer than 128 bytes |
| `401` | `missing_identity` | No `X-User-ID` header |
| `404` | `campaign_not_found` | No such campaign, or Redis holds no stock for it |
| `404` | `purchase_not_found` | No such purchase — or it belongs to somebody else |
| `500` | `internal_error` | The system could not decide; nothing is implied about the ticket |

```json
{ "error": { "code": "already_purchased",
             "message": "this account already holds a ticket for this campaign" } }
```

`404` covers two cases worth naming. If Redis has no stock counter for the
campaign, the answer is *not* `stock_exhausted`: a missing key means
reconciliation has not run, and telling a caller "sold out" in that state is a
lie that looks like a normal, final refusal — they stop trying and nobody
investigates. It is a `404` and a loud log line instead. And a purchase that
exists but belongs to somebody else is also a `404`, not a `403`, because
"this exists but is not yours" confirms that a guessed identifier is real.

Repeating a request with the same `Idempotency-Key` returns the original
response byte for byte, with `Idempotent-Replay: true` on it. That is the whole
point of the header, and [why it has to come from the client](DECISIONS.md#why-the-client-generates-the-idempotency-key)
is its own section in the decisions.

