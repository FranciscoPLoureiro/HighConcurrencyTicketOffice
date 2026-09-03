package httpapi

import (
	"context"
	"net/http"
	"strings"
)

// userIDHeader carries the caller's identity.
//
// Real authentication is out of scope for this project. The brief assumes a
// gateway has already validated a JWT and passes the subject downstream, which
// is a normal arrangement — and one that is only safe when nothing can reach
// this service except through that gateway. Exposed directly, this header lets
// anyone claim to be anyone. docs/LIMITATIONS.md states the assumption rather
// than leaving a reader to discover it here.
const userIDHeader = "X-User-ID"

// maxUserIDLength bounds what is accepted as an identity.
//
// Without it, a header of arbitrary size flows into a database query and a
// Redis set member on the hottest path in the system. The limit is generous
// for any real subject claim and rejects the rest cheaply.
const maxUserIDLength = 128

type contextKey struct{}

// userIDContextKey is unexported and of an unexported type, so no other package
// can collide with it or read the identity out of a context by guessing.
var userIDContextKey = contextKey{}

// withIdentity requires a caller identity and puts it in the request context.
func withIdentity(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID := strings.TrimSpace(r.Header.Get(userIDHeader))

		if userID == "" {
			writeError(w, http.StatusUnauthorized, "missing_identity",
				"the "+userIDHeader+" header is required")
			return
		}
		if len(userID) > maxUserIDLength {
			writeError(w, http.StatusBadRequest, "invalid_identity",
				"the "+userIDHeader+" header is too long")
			return
		}

		ctx := context.WithValue(r.Context(), userIDContextKey, userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// userIDFrom reads the identity placed by withIdentity.
//
// The boolean is not defensive padding: a handler reachable without the
// middleware would otherwise silently attribute a purchase to the empty user,
// and every such purchase would collide on the same identity.
func userIDFrom(ctx context.Context) (string, bool) {
	userID, ok := ctx.Value(userIDContextKey).(string)
	return userID, ok && userID != ""
}
