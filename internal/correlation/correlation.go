// Package correlation carries one identifier for one request across everything
// that handles it.
//
// The API accepts a purchase, the broker holds it, and a worker finishes it
// seconds later in a different process. Without a shared identifier those are
// three unrelated stories in three sets of logs, and answering "what happened
// to this ticket?" means matching timestamps by eye.
//
// It is its own package because everything needs it and nothing should have to
// import the HTTP layer to get it.
package correlation

import (
	"context"

	"github.com/google/uuid"
)

// HeaderName lets a caller supply their own identifier, so that a trace which
// started upstream carries on through this system rather than restarting here.
const HeaderName = "X-Correlation-ID"

// maxLength bounds what is accepted from a caller. The value ends up in every
// log line the request produces and in the body of a queue message, and neither
// is a place to let an unbounded header through.
const maxLength = 128

type contextKey struct{}

var correlationContextKey = contextKey{}

// New mints an identifier for a request that arrived without one.
func New() string { return uuid.NewString() }

// WithID returns a context carrying the identifier.
func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationContextKey, id)
}

// From reads the identifier, returning the empty string when there is none.
//
// It does not invent one. A caller that finds nothing here is running outside a
// request, and a fresh identifier at that point would correlate a line with
// nothing at all while looking exactly like one that does.
func From(ctx context.Context) string {
	id, _ := ctx.Value(correlationContextKey).(string)
	return id
}

// Sanitise accepts a caller-supplied identifier or replaces it with a new one.
//
// Anything empty or overlong is replaced rather than rejected. A malformed
// correlation header is not a reason to refuse somebody's purchase; it is a
// reason not to believe the value.
func Sanitise(supplied string) string {
	if supplied == "" || len(supplied) > maxLength {
		return New()
	}
	return supplied
}
