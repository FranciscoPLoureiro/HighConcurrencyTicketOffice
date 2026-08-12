package httpapi

import (
	"net/http"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/correlation"
)

// withCorrelationID gives every request an identifier and echoes it back.
//
// A purchase is answered by the API and finished by a worker in another process
// seconds later. Without one identifier running through both, the two halves of
// what happened to a ticket are two unrelated sets of log lines that have to be
// matched up by timestamp and hope.
//
// A caller-supplied header is honoured so that a trace which started upstream
// carries on rather than restarting here. It is not trusted for anything: the
// value only ever appears in logs and in a queue message, never in a decision,
// so the worst a forged one can do is make somebody's own logs confusing.
//
// Echoed in the response because a client that reports a problem can then quote
// the identifier, which turns "it failed sometime this morning" into one line
// in one file.
func withCorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := correlation.Sanitise(r.Header.Get(correlation.HeaderName))

		w.Header().Set(correlation.HeaderName, id)
		next.ServeHTTP(w, r.WithContext(correlation.WithID(r.Context(), id)))
	})
}
