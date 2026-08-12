package httpapi

import (
	"log/slog"
	"net/http"
	"time"
)

// Observer records what a request did.
//
// An interface, like Limiter and IdempotencyStore, so that the transport layer
// does not depend on Prometheus and the handler tests do not need a registry.
// A nil Observer turns the measurement off, which is what every test that is
// not about metrics uses.
type Observer interface {
	ObserveRequest(route, method string, status int, elapsed time.Duration)
}

// observedWriter remembers the status on its way past.
//
// net/http offers no way to read back what was written, and a handler that
// never calls WriteHeader has still sent a 200 — so the zero value has to be
// resolved at the end rather than trusted.
type observedWriter struct {
	http.ResponseWriter
	status int
}

func (w *observedWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *observedWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// withObserver times a request and records how it ended.
//
// The route is passed in rather than taken from r.URL.Path, and that is the
// whole reason this takes an argument. The status route contains a purchase id,
// so labelling by path would mint a new time series per ticket sold — the
// classic way to take down the monitoring system a while after the incident it
// was supposed to explain.
//
// It wraps the outermost layer of each route, so a request refused by the rate
// limiter or the identity check is measured too. A latency graph that only
// counts requests which got as far as a handler is a graph that looks healthiest
// at exactly the moment everything is being rejected.
func (s *Server) withObserver(route string, next http.Handler) http.Handler {
	if s.observer == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		observed := &observedWriter{ResponseWriter: w}

		next.ServeHTTP(observed, r)

		status := observed.status
		if status == 0 {
			status = http.StatusOK
		}
		s.observer.ObserveRequest(route, r.Method, status, time.Since(start))
	})
}

// withAccessLog writes one line per request.
//
// Separate from withObserver because they answer different questions and only
// one of them should be on for every route: metrics aggregate, so a health
// probe every five seconds costs nothing, while a log line per probe is
// seventeen thousand lines a day saying nothing happened.
//
// It exists at all because the correlation id is only useful if both ends of a
// request produce a line to join. The worker logs when it fulfils a ticket; the
// API, on a successful purchase, logged only at debug — so following a request
// across the two processes worked in exactly one direction. This is the other
// end.
//
// It must sit inside withCorrelationID, or the context it logs with has no id
// in it yet: the inner middleware attaches the id to a *derived* request, and
// that does not travel back up to whatever wrapped it.
func (s *Server) withAccessLog(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		observed := &observedWriter{ResponseWriter: w}

		next.ServeHTTP(observed, r)

		status := observed.status
		if status == 0 {
			status = http.StatusOK
		}

		// Errors at error level so that a 5xx is findable without knowing
		// which route produced it. Refusals stay at info: a 409 is this system
		// working, and logging four hundred of them as problems during a
		// campaign is how a log stops being read.
		level := slog.LevelInfo
		if status >= http.StatusInternalServerError {
			level = slog.LevelError
		}

		s.logger.Log(r.Context(), level, "request served",
			slog.String("route", route),
			slog.String("method", r.Method),
			slog.Int("status", status),
			slog.Duration("elapsed", time.Since(start)))
	})
}
