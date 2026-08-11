// Package httpapi exposes the service over HTTP.
package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/health"
)

// Purchaser sells one ticket to one user.
//
// The handler depends on this interface rather than on a concrete store so
// that phase 2 can substitute the Redis-backed implementation without the
// transport layer noticing, and so the handler's own behaviour can be tested
// against every refusal reason without a database.
type Purchaser interface {
	Purchase(ctx context.Context, campaignID, userID string) (domain.Purchase, error)
}

// Config carries the Server's dependencies.
//
// A struct rather than positional parameters: later phases add a rate limiter,
// an idempotency cache and a status reader, and a six-argument constructor is
// how callers start passing them in the wrong order.
type Config struct {
	Health     *health.Checker
	Purchaser  Purchaser
	CampaignID string
	Logger     *slog.Logger
}

// Server holds the dependencies shared by every handler.
type Server struct {
	health     *health.Checker
	purchaser  Purchaser
	campaignID string
	logger     *slog.Logger
}

// New builds a Server.
func New(cfg Config) *Server {
	return &Server{
		health:     cfg.Health,
		purchaser:  cfg.Purchaser,
		campaignID: cfg.CampaignID,
		logger:     cfg.Logger,
	}
}

// Routes returns the HTTP handler for the whole API.
//
// The standard library's ServeMux has matched on method and path pattern since
// Go 1.22, which covers everything this API needs. A third-party router would
// add a dependency and buy nothing.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", s.handleHealth)
	// Identity is required for the purchase path and meaningless for health,
	// so the middleware wraps the one route rather than the whole mux.
	mux.Handle("POST /api/v1/tickets/purchase", withIdentity(http.HandlerFunc(s.handlePurchase)))

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
