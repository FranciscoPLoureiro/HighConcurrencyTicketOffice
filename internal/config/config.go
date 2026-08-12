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

	// RabbitMQURL addresses the broker that carries a sold ticket to the
	// worker that finishes it.
	RabbitMQURL string

	// PublisherChannels is how many confirmed publishes the API may have in
	// flight, and WorkerPrefetch is how many unacknowledged messages one
	// worker will hold.
	PublisherChannels int
	WorkerPrefetch    int

	// FulfilmentDelay stands in for generating a PDF. It is configuration
	// rather than a constant so that the integration tests do not have to
	// wait two seconds per message to prove something unrelated to waiting.
	FulfilmentDelay time.Duration

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

	// IdempotencyLease is how long an unfinished request holds its key
	// against a retry, and IdempotencyRetention is how long the answer it
	// produced stays replayable.
	IdempotencyLease     time.Duration
	IdempotencyRetention time.Duration

	// The budgets every outbound call runs under. Nothing this service does
	// is allowed to wait indefinitely on another system: a caller who has
	// given up must stop costing resources, and a dependency that has
	// stopped answering must fail rather than accumulate goroutines holding
	// pool connections until the process falls over.
	//
	// RequestTimeout bounds a whole purchase, and the three below bound the
	// individual calls inside it. They are deliberately shorter than it, so
	// that a single slow dependency is reported as that dependency being
	// slow rather than as the request as a whole timing out.
	RequestTimeout  time.Duration
	RedisTimeout    time.Duration
	PostgresTimeout time.Duration
	PublishTimeout  time.Duration

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

	// A lease has to outlast the request it is protecting, or a retry
	// arriving while the first attempt is still working claims a key that
	// was never free and buys a second ticket. Thirty seconds is twice the
	// server's own write timeout, so a request that is still running has
	// already been abandoned by the HTTP layer.
	defaultIdempotencyLease = 30 * time.Second
	// Retention answers a different question: how long after giving up might
	// somebody try again? A day covers a client that retried after a crash,
	// a phone that regained signal, or a person who reopened the tab in the
	// morning, and costs a few hundred bytes per purchase to do it.
	defaultIdempotencyRetention = 24 * time.Hour

	defaultRabbitMQURL = "amqp://tickets:tickets@localhost:5672/" //nolint:gosec // G101: documented development default, as above

	// Eight channels is comfortably more than the campaign needs — a hundred
	// winners across the whole burst — and bounded so that a pathological
	// load cannot ask the broker for a channel per request.
	defaultPublisherChannels = 8
	// Prefetch is per worker. Small enough that a second worker starting
	// mid-campaign has something to do rather than watching the first work
	// through a backlog it has already claimed, large enough that a worker
	// is never idle waiting for the next message to be pushed. Fulfilment
	// takes seconds, so the round trip this saves is noise either way and
	// the spreading is the entire benefit.
	defaultWorkerPrefetch = 4
	// The brief's two seconds of pretending to draw a PDF.
	defaultFulfilmentDelay = 2 * time.Second

	// A purchase touches Redis once, PostgreSQL once and the broker once, so
	// the sum of the three budgets below is the worst case, and the request
	// budget sits above it with room for the handler itself.
	defaultRequestTimeout = 10 * time.Second
	// Redis is in the same datacentre and every operation it is asked for is
	// a single script over a handful of keys. Two seconds is already far
	// beyond healthy; anything slower is an incident, not a slow query.
	defaultRedisTimeout = 2 * time.Second
	// PostgreSQL gets longer because its work includes waiting on a row lock
	// that another transaction holds, which is legitimate contention rather
	// than a failure.
	defaultPostgresTimeout = 5 * time.Second
	// A confirmed publish waits for the broker to fsync, so this covers a
	// disk that is briefly busy rather than only the network.
	defaultPublishTimeout = 5 * time.Second
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
		RabbitMQURL:     stringVar("RABBITMQ_URL", defaultRabbitMQURL),
		ShutdownTimeout: durationVar("SHUTDOWN_TIMEOUT", defaultShutdownTimeout, &errs),
		LogLevel:        levelVar("LOG_LEVEL", defaultLogLevel, &errs),
		CampaignID:      stringVar("CAMPAIGN_ID", defaultCampaignID),
		TotalTickets:    intVar("TOTAL_TICKETS", defaultTotalTickets, &errs),
		RateLimitWindow: durationVar("RATE_LIMIT_WINDOW", defaultRateLimitWindow, &errs),
		RateLimitUser:   intVar("RATE_LIMIT_USER", defaultRateLimitUser, &errs),
		RateLimitIP:     intVar("RATE_LIMIT_IP", defaultRateLimitIP, &errs),

		IdempotencyLease:     durationVar("IDEMPOTENCY_LEASE", defaultIdempotencyLease, &errs),
		IdempotencyRetention: durationVar("IDEMPOTENCY_RETENTION", defaultIdempotencyRetention, &errs),

		PublisherChannels: intVar("PUBLISHER_CHANNELS", defaultPublisherChannels, &errs),
		WorkerPrefetch:    intVar("WORKER_PREFETCH", defaultWorkerPrefetch, &errs),
		FulfilmentDelay:   durationVar("FULFILMENT_DELAY", defaultFulfilmentDelay, &errs),

		RequestTimeout:  durationVar("REQUEST_TIMEOUT", defaultRequestTimeout, &errs),
		RedisTimeout:    durationVar("REDIS_TIMEOUT", defaultRedisTimeout, &errs),
		PostgresTimeout: durationVar("POSTGRES_TIMEOUT", defaultPostgresTimeout, &errs),
		PublishTimeout:  durationVar("PUBLISH_TIMEOUT", defaultPublishTimeout, &errs),
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
	// Neither of these may be switched off. A zero lease claims a key that
	// expires before the request it protects finishes, and a zero retention
	// keeps no answer to replay — in both cases the endpoint silently stops
	// being idempotent while still demanding the header that says it is.
	if c.IdempotencyLease <= 0 {
		errs = append(errs, fmt.Errorf("IDEMPOTENCY_LEASE must be positive, got %s", c.IdempotencyLease))
	}
	if c.IdempotencyRetention <= 0 {
		errs = append(errs, fmt.Errorf("IDEMPOTENCY_RETENTION must be positive, got %s", c.IdempotencyRetention))
	}
	if c.RabbitMQURL == "" {
		errs = append(errs, errors.New("RABBITMQ_URL must not be empty"))
	}
	if c.PublisherChannels <= 0 {
		errs = append(errs, fmt.Errorf("PUBLISHER_CHANNELS must be positive, got %d", c.PublisherChannels))
	}
	if c.WorkerPrefetch <= 0 {
		errs = append(errs, fmt.Errorf("WORKER_PREFETCH must be positive, got %d", c.WorkerPrefetch))
	}
	// Zero is legitimate: it is what an integration test asks for when the
	// point of the test is not the waiting. Negative is a typo.
	if c.FulfilmentDelay < 0 {
		errs = append(errs, fmt.Errorf("FULFILMENT_DELAY must not be negative, got %s", c.FulfilmentDelay))
	}
	// A timeout of zero is not "no limit" here, it is "give up immediately",
	// and either reading would be a surprise. Neither is offered.
	for _, budget := range []struct {
		name  string
		value time.Duration
	}{
		{"REQUEST_TIMEOUT", c.RequestTimeout},
		{"REDIS_TIMEOUT", c.RedisTimeout},
		{"POSTGRES_TIMEOUT", c.PostgresTimeout},
		{"PUBLISH_TIMEOUT", c.PublishTimeout},
	} {
		if budget.value <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive, got %s", budget.name, budget.value))
		}
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
