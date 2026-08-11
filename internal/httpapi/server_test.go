package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/health"
)

func testRoutes(probes ...health.Probe) http.Handler {
	return New(health.New(probes...), slog.New(slog.DiscardHandler)).Routes()
}

func get(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestHealthReturns200WhenEveryDependencyAnswers(t *testing.T) {
	routes := testRoutes(
		health.Probe{Name: "postgres", Ping: func(context.Context) error { return nil }},
		health.Probe{Name: "redis", Ping: func(context.Context) error { return nil }},
	)

	rec := get(t, routes, "/health")

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q, want JSON", got)
	}

	var report health.Report
	if err := json.NewDecoder(rec.Body).Decode(&report); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	if report.Status != health.StatusOK {
		t.Errorf("status = %q, want %q", report.Status, health.StatusOK)
	}
	for _, name := range []string{"postgres", "redis"} {
		if _, ok := report.Checks[name]; !ok {
			t.Errorf("body omits the %q dependency", name)
		}
	}
}

// An orchestrator should be able to act on the status line alone, without
// parsing and interpreting the payload to discover the instance is unusable.
func TestHealthReturns503WhenADependencyIsDown(t *testing.T) {
	routes := testRoutes(
		health.Probe{Name: "postgres", Ping: func(context.Context) error { return errors.New("connection refused") }},
		health.Probe{Name: "redis", Ping: func(context.Context) error { return nil }},
	)

	rec := get(t, routes, "/health")

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	var report health.Report
	if err := json.NewDecoder(rec.Body).Decode(&report); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	// The body still has to say which dependency failed — that is the
	// difference between an alert someone can act on and one they cannot.
	if got := report.Checks["postgres"].Status; got != health.StatusUnavailable {
		t.Errorf("postgres = %q, want %q", got, health.StatusUnavailable)
	}
	if got := report.Checks["redis"].Status; got != health.StatusOK {
		t.Errorf("redis = %q, want %q", got, health.StatusOK)
	}
}

func TestHealthRejectsNonGETMethods(t *testing.T) {
	routes := testRoutes()

	rec := httptest.NewRecorder()
	routes.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/health", nil))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /health = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestUnknownPathsReturn404(t *testing.T) {
	if rec := get(t, testRoutes(), "/does-not-exist"); rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}
