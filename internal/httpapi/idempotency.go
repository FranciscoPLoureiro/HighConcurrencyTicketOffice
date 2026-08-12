package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/google/uuid"
)

// idempotencyKeyHeader carries the client's own identifier for the request.
//
// The client generates it, not the server, and that is the whole point. A
// server-generated key differs on every attempt, so it protects against a
// broker redelivering a message and against nothing else. The failure worth
// protecting against is the one the client can see: a request that timed out
// with no answer, where the only thing the client can do is send it again, and
// the only thing the server can do is recognise it.
const idempotencyKeyHeader = "Idempotency-Key"

// replayHeader marks a response that was recorded earlier rather than produced
// now. Nothing in the protocol requires it; it is here so that a person
// debugging a retry can see which of the two answers they are looking at.
const replayHeader = "Idempotent-Replay"

// IdempotencyStore remembers what a key has already produced.
//
// An interface for the same reason Limiter is one: the transport layer should
// not depend on Redis, and the middleware's own behaviour — the codes, the
// header, what happens to a key when the handler fails — is worth testing
// without a container.
type IdempotencyStore interface {
	ClaimIdempotency(ctx context.Context, key string, lease time.Duration) (domain.Claim, string, error)
	StoreResponse(ctx context.Context, key, response string, retention time.Duration) error
	ReleaseClaim(ctx context.Context, key string) error
}

// IdempotencyPolicy is how long a key is held and how long its answer is kept.
type IdempotencyPolicy struct {
	// Lease bounds how long an attempt that never finished may block a
	// retry. Comfortably longer than a request can take, and much shorter
	// than a person's patience: too short and a slow first attempt lets a
	// retry through to buy a second ticket, too long and a caller whose
	// request died with the process is locked out for no reason.
	Lease time.Duration
	// Retention is how long after the fact a retry is still recognised as
	// one. Past it the caller gets the ordinary refusal for somebody who
	// already holds a ticket — true, and less useful than the original
	// answer.
	Retention time.Duration
}

// Enabled reports whether the policy does anything.
func (p IdempotencyPolicy) Enabled() bool { return p.Lease > 0 && p.Retention > 0 }

// storedResponse is the recorded answer, kept whole so a replay is what the
// first attempt received rather than a reconstruction of it.
//
// Headers are part of "whole". A successful purchase answers 202 with a
// Location pointing at the status resource, and a replay that dropped it would
// hand the retry a 202 with nowhere to go — in the one case the client is most
// likely to be following the header, having never seen the first response at
// all. Omitted when empty, and absent from records written before this field
// existed, which decode to no headers and replay as they always did.
type storedResponse struct {
	Status  int                 `json:"status"`
	Headers map[string][]string `json:"headers,omitempty"`
	Body    json.RawMessage     `json:"body"`
}

// capturingWriter records a response on its way out.
//
// It buffers rather than tees because the answer has to be stored before it can
// be promised, and a body already on the wire cannot be taken back if storing
// it fails. Purchase responses are two hundred bytes; the buffering is not the
// interesting cost here.
type capturingWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *capturingWriter) Header() http.Header { return w.header }

func (w *capturingWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *capturingWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(b)
}

// withIdempotency makes a repeated request return its first answer.
//
// The key is required rather than optional. Stripe and friends treat it as
// opt-in because most of their endpoints are safe to repeat; this one consumes
// a finite thing, so a request without a key is a request nobody can retry
// safely, and accepting it would leave the guarantee depending on whether the
// client remembered to ask for it.
//
// Note what this does and does not protect. The one-ticket-per-user rule is
// enforced in the Lua script and holds with or without a key: a retry that
// invents a new key still gets refused. What the key buys is the *answer* — the
// difference between a client learning "you already have a ticket, and here is
// nothing about it" and learning "here is the ticket you bought, again".
func (s *Server) withIdempotency(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.idempotency == nil || !s.idempotencyPolicy.Enabled() {
			next.ServeHTTP(w, r)
			return
		}

		key, ok := s.readIdempotencyKey(w, r)
		if !ok {
			return
		}

		userID, _ := userIDFrom(r.Context())
		recordKey := s.idempotencyKey(s.campaignID, userID, key)

		claim, stored, err := s.idempotency.ClaimIdempotency(r.Context(), recordKey, s.idempotencyPolicy.Lease)
		if err != nil {
			// Fail closed, unlike the rate limiter.
			//
			// A limiter that cannot answer costs the system some protection
			// from load. This, unable to answer, cannot tell a first attempt
			// from a retry — so letting the request through is a coin toss
			// on whether somebody is charged a second ticket, and the whole
			// mechanism exists to remove that coin toss.
			s.writeInternalError(w, "could not claim the idempotency key", err)
			return
		}

		switch claim {
		case domain.ClaimReplayed:
			s.replay(w, recordKey, stored)
			return

		case domain.ClaimInFlight:
			// 409 rather than 202: there is no correlation id to hand over,
			// because the attempt that will produce it has not finished. The
			// client should wait and repeat the same request, which is
			// exactly what the key makes safe.
			writeError(w, http.StatusConflict, "idempotency_key_in_use",
				"an earlier request with this "+idempotencyKeyHeader+" is still being processed")
			return

		case domain.ClaimAccepted:
		}

		capture := &capturingWriter{header: make(http.Header)}
		next.ServeHTTP(capture, r)

		s.record(r.Context(), recordKey, capture)
		copyResponse(w, capture)
	})
}

// readIdempotencyKey pulls the key off the request and validates it.
func (s *Server) readIdempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := strings.TrimSpace(r.Header.Get(idempotencyKeyHeader))

	if key == "" {
		writeError(w, http.StatusBadRequest, "missing_idempotency_key",
			"the "+idempotencyKeyHeader+" header is required and must be a UUID")
		return "", false
	}
	// A UUID because the brief asks for one, and because requiring a shape
	// bounds the key's length on the way into a Redis key name for free. A
	// caller who sends a counter would collide with every other caller who
	// had the same idea.
	if _, err := uuid.Parse(key); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_idempotency_key",
			"the "+idempotencyKeyHeader+" header must be a UUID")
		return "", false
	}

	return key, true
}

// replay writes back the answer an earlier attempt produced.
func (s *Server) replay(w http.ResponseWriter, recordKey, stored string) {
	var response storedResponse
	if err := json.Unmarshal([]byte(stored), &response); err != nil {
		// The record exists but cannot be read, which is a bug in what was
		// written rather than anything the caller did. Serving the request
		// again could take a second ticket, so it does not.
		s.writeInternalError(w, "stored idempotent response is unreadable", err)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	for name, values := range response.Headers {
		w.Header()[name] = values
	}
	// Last, so that a recorded header cannot claim this was not a replay.
	w.Header().Set(replayHeader, "true")
	w.WriteHeader(response.Status)

	if _, err := w.Write(response.Body); err != nil {
		s.logger.Error("failed to write a replayed response",
			slog.String("key", recordKey),
			slog.Any("error", err))
	}
}

// record keeps the answer, or gives the key back when there is no answer worth
// keeping.
func (s *Server) record(ctx context.Context, recordKey string, capture *capturingWriter) {
	// A detached context. The usual reason to be here on a failure is that
	// the caller gave up, and inheriting that context would mean the key is
	// neither stored nor released — leaving the caller locked out until the
	// lease expires, in exactly the case they are most likely to retry.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.idempotencyPolicy.Lease)
	defer cancel()

	// 5xx is not an answer. The request may have done anything or nothing,
	// and the client's only recourse is to send it again — with the same key,
	// which has to be free for that to be worth anything.
	//
	// A refusal is an answer and is kept. "You already hold a ticket" and
	// "there are none left" are decisions the system made, and a retry should
	// hear the same decision rather than race for a different one.
	if capture.status >= http.StatusInternalServerError {
		if err := s.idempotency.ReleaseClaim(ctx, recordKey); err != nil {
			s.logger.Error("could not release an idempotency key after a failure",
				slog.String("key", recordKey),
				slog.Any("error", err))
		}
		return
	}

	body := capture.body.Bytes()
	if !json.Valid(body) {
		body = []byte("null")
	}

	encoded, err := json.Marshal(storedResponse{
		Status:  capture.status,
		Headers: capture.header,
		Body:    body,
	})
	if err != nil {
		s.logger.Error("could not encode a response for replay",
			slog.String("key", recordKey),
			slog.Any("error", err))
		return
	}

	if err := s.idempotency.StoreResponse(ctx, recordKey, string(encoded), s.idempotencyPolicy.Retention); err != nil {
		// The purchase happened; only the memory of it failed. Said out loud
		// because the consequence is invisible until the client retries and
		// gets a refusal instead of their ticket.
		s.logger.Error("could not store a response for replay",
			slog.String("key", recordKey),
			slog.Any("error", err))
	}
}

// copyResponse sends the captured response to the real client.
func copyResponse(w http.ResponseWriter, capture *capturingWriter) {
	for name, values := range capture.header {
		w.Header()[name] = values
	}

	status := capture.status
	if status == 0 {
		// A handler that wrote nothing at all. net/http would send 200 here,
		// and so does this.
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(capture.body.Bytes())
}
