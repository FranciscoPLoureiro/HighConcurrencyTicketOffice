// Package config loads and validates process configuration from the environment.
//
// Configuration is read once at startup and validated there. A deployment
// missing its database URL should fail immediately and say which variable is
// wrong, rather than starting healthy and collapsing on the first request that
// happens to need the value.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the fully resolved configuration for a single process.
type Config struct {
	// HTTPAddr is the listen address for the API server.
	HTTPAddr string

	// ShutdownTimeout bounds how long a process waits for in-flight work
	// after SIGTERM before giving up and exiting anyway.
	ShutdownTimeout time.Duration

	// PostgresDSN addresses the source of truth. Every invariant that must
	// survive a restart is ultimately recoverable from here.
	PostgresDSN string

	// RedisAddr and RedisPassword address the cache that guards the stock.
	RedisAddr     string
	RedisPassword string

	// CampaignID names the campaign this process serves, and TotalTickets
	// is how many exist. The campaign row is created from these once and
	// never reset, so changing TotalTickets does not resize a campaign that
	// has already started selling.
	CampaignID   string
	TotalTickets int

	// RateLimitWindow is the period both limits below are measured over,
	// and RateLimitUser and RateLimitIP are how many purchase attempts one
	// account and one address may make within it. Either limit set to zero
	// disables that check.
	RateLimitWindow time.Duration
	RateLimitUser   int
	RateLimitIP     int

	// LogLevel is the minimum level emitted by the structured logger.
	LogLevel slog.Level
}

// Defaults match docker-compose.yml so that `make up` followed by
// `go run ./cmd/api` works with no further setup.
//
// The credentials below are throwaway development values, committed
// deliberately and used nowhere else: a deployment supplies every one of them
// through the environment. Treating them as secrets would buy nothing and cost
// every newcomer a broken first run.
const (
	defaultHTTPAddr        = ":8080"
	defaultShutdownTimeout = 15 * time.Second
	// gosec flags the inline password (G101) and it is right to: a
	// credential in source is normally a real finding. It is accepted here
	// because this exact pair is the one docker-compose creates for a
	// throwaway local database, no deployment ever reads it, and the
	// alternative — no default — costs every newcomer a broken first run.
	defaultPostgresDSN = "postgres://tickets:tickets@localhost:5432/tickets?sslmode=disable" //nolint:gosec // G101: documented development default, see above
	defaultRedisAddr   = "localhost:6379"
	defaultLogLevel    = slog.LevelInfo
	defaultCampaignID  = "queima-2026"
	// The brief's campaign: 100 tickets at 80% off.
	defaultTotalTickets = 100

	defaultRateLimitWindow = 10 * time.Second
	// Five attempts in ten seconds is far above what pressing a button
	// produces and far below what a script produces. The limit exists to
	// stop one account retrying in a loop, not to police impatience.
	defaultRateLimitUser = 5
	// The address limit is deliberately loose. The campaign's audience is a
	// university, where thousands of students share a handful of NAT
	// addresses, so a tight per-IP limit does not stop an attacker — it
	// stops a hall of residence. It is set to absorb the entire expected
	// burst from one address and catch only a single machine going orders
	// of magnitude beyond human speed.
	//
	// This interacts with the load test, which drives every virtual user
	// from one container and therefore one address: raise VUS above this
	// and the generator starts rate limiting itself. That is the limiter
	// working, and the k6 output counts the two refusal reasons separately
	// so the difference is visible rather than mysterious.
	defaultRateLimitIP = 1000
)

// Load reads configuration from the process environment.
//
// Errors accumulate instead of short-circuiting. An operator repairing a broken
// deployment should see every problem in one pass, not discover them one
// restart at a time.
func Load() (Config, error) {
	var errs []error

	cfg := Config{
		HTTPAddr:        stringVar("HTTP_ADDR", defaultHTTPAddr),
		PostgresDSN:     stringVar("POSTGRES_DSN", defaultPostgresDSN),
		RedisAddr:       stringVar("REDIS_ADDR", defaultRedisAddr),
		RedisPassword:   stringVar("REDIS_PASSWORD", ""),
		ShutdownTimeout: durationVar("SHUTDOWN_TIMEOUT", defaultShutdownTimeout, &errs),
		LogLevel:        levelVar("LOG_LEVEL", defaultLogLevel, &errs),
		CampaignID:      stringVar("CAMPAIGN_ID", defaultCampaignID),
		TotalTickets:    intVar("TOTAL_TICKETS", defaultTotalTickets, &errs),
		RateLimitWindow: durationVar("RATE_LIMIT_WINDOW", defaultRateLimitWindow, &errs),
		RateLimitUser:   intVar("RATE_LIMIT_USER", defaultRateLimitUser, &errs),
		RateLimitIP:     intVar("RATE_LIMIT_IP", defaultRateLimitIP, &errs),
	}

	errs = append(errs, cfg.validate()...)

	return cfg, errors.Join(errs...)
}

// validate rejects values that parse but cannot work.
func (c Config) validate() []error {
	var errs []error

	if c.HTTPAddr == "" {
		errs = append(errs, errors.New("HTTP_ADDR must not be empty"))
	}
	if c.PostgresDSN == "" {
		errs = append(errs, errors.New("POSTGRES_DSN must not be empty"))
	}
	if c.RedisAddr == "" {
		errs = append(errs, errors.New("REDIS_ADDR must not be empty"))
	}
	// A zero or negative shutdown timeout would make graceful shutdown a
	// no-op, silently turning every deploy into a hard kill of in-flight work.
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, fmt.Errorf("SHUTDOWN_TIMEOUT must be positive, got %s", c.ShutdownTimeout))
	}
	if c.CampaignID == "" {
		errs = append(errs, errors.New("CAMPAIGN_ID must not be empty"))
	}
	// A campaign with no tickets is not a campaign, and a negative one would
	// pass the schema's own CHECK only by accident.
	if c.TotalTickets <= 0 {
		errs = append(errs, fmt.Errorf("TOTAL_TICKETS must be positive, got %d", c.TotalTickets))
	}
	// Zero disables a limit, which is a legitimate choice. A negative one is
	// a typo that would read as "disabled" and silently remove a control
	// somebody believed was on.
	if c.RateLimitUser < 0 {
		errs = append(errs, fmt.Errorf("RATE_LIMIT_USER must not be negative, got %d", c.RateLimitUser))
	}
	if c.RateLimitIP < 0 {
		errs = append(errs, fmt.Errorf("RATE_LIMIT_IP must not be negative, got %d", c.RateLimitIP))
	}
	if c.RateLimitWindow <= 0 {
		errs = append(errs, fmt.Errorf("RATE_LIMIT_WINDOW must be positive, got %s", c.RateLimitWindow))
	}

	return errs
}

// stringVar reads key, falling back when it is unset or blank.
//
// Blank is treated as unset because container orchestrators routinely inject
// empty strings for variables that were never given a value, and an empty
// listen address is never what the operator meant.
func stringVar(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			return trimmed
		}
	}
	return fallback
}

func durationVar(key string, fallback time.Duration, errs *[]error) time.Duration {
	raw := stringVar(key, "")
	if raw == "" {
		return fallback
	}

	d, err := time.ParseDuration(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a duration (want e.g. 15s, 1m): %w", key, raw, err))
		return fallback
	}
	return d
}

func levelVar(key string, fallback slog.Level, errs *[]error) slog.Level {
	raw := stringVar(key, "")
	if raw == "" {
		return fallback
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a log level (want debug, info, warn or error): %w", key, raw, err))
		return fallback
	}
	return level
}

func intVar(key string, fallback int, errs *[]error) int {
	raw := stringVar(key, "")
	if raw == "" {
		return fallback
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not an integer: %w", key, raw, err))
		return fallback
	}
	return n
}
