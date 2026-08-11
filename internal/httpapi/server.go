// Package httpapi exposes the service over HTTP.
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/health"
)

// Server holds the dependencies shared by every handler.
type Server struct {
	health *health.Checker
	logger *slog.Logger
}

// New builds a Server.
func New(checker *health.Checker, logger *slog.Logger) *Server {
	return &Server{health: checker, logger: logger}
}

// Routes returns the HTTP handler for the whole API.
//
// The standard library's ServeMux has matched on method and path pattern since
// Go 1.22, which covers everything this API needs. A third-party router would
// add a dependency and buy nothing.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	report := s.health.Check(r.Context())

	status := http.StatusOK
	if !report.Healthy() {
		// 503 rather than 200-with-a-sad-body, so that an orchestrator
		// takes the instance out of rotation without having to parse
		// and interpret the payload.
		status = http.StatusServiceUnavailable
	}

	s.writeJSON(w, status, report)
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line and headers are already on the wire, so there
		// is no way to turn this into an error response. Logging it is
		// the only remaining option, and silence here would hide a
		// genuine serialisation bug.
		s.logger.Error("failed to encode response body", slog.Any("error", err))
	}
}
