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
	Purchase(ctx context.Context, campaignID, userID, idempotencyKey string) (domain.Purchase, error)
	ReadPurchase(ctx context.Context, campaignID, purchaseID string) (domain.Purchase, error)
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

	// Limiter and the two policies below are optional: a zero Policy
	// disables its check, which keeps the handler tests free of a Redis
	// they are not testing.
	Limiter   Limiter
	UserLimit Policy
	IPLimit   Policy

	// RateLimitKey names a caller's bucket. Injected so that the transport
	// layer does not have to know how the cache namespaces its keys.
	RateLimitKey func(scope, id string) string

	// Idempotency and its policy are optional in the same way: a nil store
	// or a zero policy turns the check off, which is what keeps the handler
	// tests that are not about replay free of one.
	Idempotency       IdempotencyStore
	IdempotencyPolicy IdempotencyPolicy

	// IdempotencyKey names a caller's record, for the same reason
	// RateLimitKey is injected.
	IdempotencyKey func(campaignID, userID, key string) string

	// Observer records latency and outcome per route. Optional: a nil one
	// turns the measurement off, which is what the handler tests use.
	Observer Observer

	// Metrics, when set, is exposed at /metrics. Separate from Observer
	// because one is what the server reports and the other is where it
	// reports it, and a process can do either without the other — the worker
	// serves metrics and no routes at all.
	Metrics http.Handler
}

// Server holds the dependencies shared by every handler.
type Server struct {
	health            *health.Checker
	purchaser         Purchaser
	campaignID        string
	logger            *slog.Logger
	limiter           Limiter
	userLimit         Policy
	ipLimit           Policy
	rateLimitKey      func(scope, id string) string
	idempotency       IdempotencyStore
	idempotencyPolicy IdempotencyPolicy
	idempotencyKey    func(campaignID, userID, key string) string
	observer          Observer
	metrics           http.Handler
}

// New builds a Server.
func New(cfg Config) *Server {
	s := &Server{
		health:            cfg.Health,
		purchaser:         cfg.Purchaser,
		campaignID:        cfg.CampaignID,
		logger:            cfg.Logger,
		limiter:           cfg.Limiter,
		userLimit:         cfg.UserLimit,
		ipLimit:           cfg.IPLimit,
		rateLimitKey:      cfg.RateLimitKey,
		idempotency:       cfg.Idempotency,
		idempotencyPolicy: cfg.IdempotencyPolicy,
		idempotencyKey:    cfg.IdempotencyKey,
		observer:          cfg.Observer,
		metrics:           cfg.Metrics,
	}
	if s.rateLimitKey == nil {
		s.rateLimitKey = func(scope, id string) string { return scope + ":" + id }
	}
	if s.idempotencyKey == nil {
		s.idempotencyKey = func(campaignID, userID, key string) string {
			return campaignID + ":" + userID + ":" + key
		}
	}
	return s
}

// Routes returns the HTTP handler for the whole API.
//
// The standard library's ServeMux has matched on method and path pattern since
// Go 1.22, which covers everything this API needs. A third-party router would
// add a dependency and buy nothing.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// Measured but not logged. A container probe every five seconds is free
	// to aggregate and seventeen thousand lines a day to narrate.
	mux.Handle("GET /health", s.withObserver("/health", http.HandlerFunc(s.handleHealth)))

	if s.metrics != nil {
		// Deliberately unobserved. Scraping is not traffic, and counting it
		// would put a request every fifteen seconds into the same histogram
		// the purchase path is judged by — quietly pulling the percentiles
		// towards whatever the scrape costs.
		mux.Handle("GET /metrics", s.metrics)
	}

	// Every request gets a correlation id before anything else looks at it,
	// including the ones that are about to be refused: "why did this caller
	// get a 429?" is a question worth being able to answer, and it cannot be
	// asked of a line with nothing to join on.
	// Identity is required for the purchase path and meaningless for health,
	// so the middleware wraps the one route rather than the whole mux. The
	// rate limit sits inside the identity check, which is what lets it key
	// on the caller as well as on the address — see withRateLimit.
	//
	// Idempotency sits inside the rate limit rather than outside it. A retry
	// storm should still be throttled, and the per-user budget is generous
	// enough that an honest client repeating a timed-out request is nowhere
	// near it. The other order would make a claimed key the cheapest way to
	// bypass the limiter.
	// Correlation is outermost, so everything below it — including the
	// access log and any line a refused request writes — carries the id.
	// Wrapping it the other way round would leave the outer layers logging
	// with a context that has no id in it yet.
	mux.Handle("POST /api/v1/tickets/purchase",
		withCorrelationID(
			s.withObserver("/api/v1/tickets/purchase",
				s.withAccessLog("/api/v1/tickets/purchase",
					withIdentity(s.withRateLimit(
						s.withIdempotency(http.HandlerFunc(s.handlePurchase))))))))

	// Where a caller watches the purchase the 202 promised them. No rate
	// limit and no idempotency: it takes nothing, changes nothing, and
	// polling it is the behaviour the 202 asked for.
	// The route pattern, not the path. Labelling by path would mint a time
	// series per purchase id, which is how a metrics backend is taken down
	// some time after the incident it was meant to explain.
	mux.Handle("GET /api/v1/tickets/{id}/status",
		withCorrelationID(
			s.withObserver("/api/v1/tickets/{id}/status",
				s.withAccessLog("/api/v1/tickets/{id}/status",
					withIdentity(http.HandlerFunc(s.handlePurchaseStatus))))))

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
