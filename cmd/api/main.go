// Command api serves the ticket office HTTP API.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/cache"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/config"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/correlation"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/health"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/httpapi"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/metrics"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/purchase"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/queue"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/store"
)

func main() {
	// The runtime image is distroless: no shell, no curl, nothing for a
	// container healthcheck to execute. Re-invoking this same binary keeps
	// the image free of a toolchain an attacker could pivot to, without
	// giving up a real readiness probe.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(probeSelf())
	}

	if err := run(); err != nil {
		slog.Error("api exited with an error", slog.Any("error", err))
		os.Exit(1)
	}
}

// startupBudget bounds everything between opening the dependencies and
// listening: migrations, creating the campaign, and reconciling Redis. Each of
// those can block on something outside this process — an advisory lock, a row
// lock, a distributed lock held by a peer — and a deploy that hangs with no
// listener and no explanation is worse than one that exits and is restarted.
const startupBudget = 2 * time.Minute

// migrationBudget bounds how long startup waits for a database that is not yet
// accepting connections, or for a peer instance that is still migrating, before
// giving up and letting the platform restart us.
const migrationBudget = 30 * time.Second

// migrate applies the schema, retrying while the database is still coming up.
func migrate(ctx context.Context, db *store.Store, logger *slog.Logger) (int64, error) {
	// The budget has to be a deadline on the call itself, not a clock checked
	// between attempts. Migrate blocks on a Postgres advisory lock, and
	// pg_advisory_lock waits for as long as its context allows — so a peer
	// that holds the lock and is wedged, or one whose session outlived it,
	// parks this process inside a single attempt forever. Checking the time
	// afterwards only bounds a sequence of attempts that each fail fast,
	// which is the case that was never the problem.
	ctx, cancel := context.WithTimeout(ctx, migrationBudget)
	defer cancel()

	for attempt := 1; ; attempt++ {
		version, err := db.Migrate(ctx)
		if err == nil {
			return version, nil
		}
		// Either the budget ran out or the process is shutting down. Both
		// are terminal, and both are worth telling apart from the failure
		// that was being retried, so the message carries all three.
		if ctx.Err() != nil {
			return 0, fmt.Errorf("%w after %d attempts: %w", ctx.Err(), attempt, err)
		}

		logger.Warn("migration attempt failed, retrying",
			slog.Int("attempt", attempt),
			slog.Any("error", err))

		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("%w after %d attempts: %w", ctx.Err(), attempt, err)
		case <-time.After(2 * time.Second):
		}
	}
}

// brokerDialBudget bounds how long startup waits for RabbitMQ.
//
// Longer than the database's, because the Erlang VM takes far longer to become
// useful than PostgreSQL does and a cold `docker compose up` routinely spends
// forty seconds there while doing nothing wrong.
const brokerDialBudget = 90 * time.Second

// dialBroker connects and declares the topology, retrying while the broker is
// still coming up.
func dialBroker(ctx context.Context, url string, logger *slog.Logger) (*queue.Connection, error) {
	ctx, cancel := context.WithTimeout(ctx, brokerDialBudget)
	defer cancel()

	for attempt := 1; ; attempt++ {
		conn, err := queue.Dial(ctx, url)
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%w after %d attempts: %w", ctx.Err(), attempt, err)
		}

		logger.Warn("rabbitmq not ready, retrying",
			slog.Int("attempt", attempt),
			slog.Any("error", err))

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w after %d attempts: %w", ctx.Err(), attempt, err)
		case <-time.After(2 * time.Second):
		}
	}
}

// withRequestTimeout puts a deadline on every request before any handler sees
// it.
//
// The per-dependency budgets inside the service bound each outbound call; this
// bounds the whole. Without it a request that somehow passes every individual
// check can still sit forever, and the goroutine serving it keeps its pool
// connection and its idempotency claim for exactly as long.
//
// http.TimeoutHandler is not used because it writes its own plain-text 503 over
// whatever the handler produced, which would break the JSON error contract
// every other refusal honours. Cancelling the context instead lets the handler
// fail in its own vocabulary.
func withRequestTimeout(budget time.Duration, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), budget)
		defer cancel()

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// probeSelf performs the container healthcheck and returns a process exit code.
func probeSelf() int {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
		return 1
	}

	// A listen address is commonly written as ":8080", which is not a valid
	// host to dial.
	addr := cfg.HTTPAddr
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + addr + "/health") //nolint:noctx // the client timeout is the deadline
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
		return 1
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck: /health returned %d\n", resp.StatusCode)
		return 1
	}
	return 0
}

// prepare brings the system to a state where it can sell a ticket.
//
// All three steps block startup, and all three should. A process with no
// schema, no campaign row, or a Redis that has not been reconciled cannot serve
// a correct purchase, so there is nothing to stay up for — and the third one is
// the reason this function exists as a unit: serving before reconciliation
// finishes means answering with whatever the last run happened to leave behind.
func prepare(ctx context.Context, cfg config.Config, db *store.Store, sales *purchase.Service, logger *slog.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, startupBudget)
	defer cancel()

	// Retries rather than exiting on the first failure because a rolling
	// deploy routinely starts the app seconds before the database begins
	// accepting connections, and a crash loop there is noise, not signal.
	version, err := migrate(ctx, db, logger)
	if err != nil {
		return fmt.Errorf("migrate schema: %w", err)
	}
	logger.Info("schema up to date", slog.Int64("version", version))

	if err := db.EnsureCampaign(ctx, cfg.CampaignID, cfg.TotalTickets); err != nil {
		return fmt.Errorf("ensure campaign: %w", err)
	}

	// The most important line in this function. Without it, restarting the
	// service mid-campaign either resells tickets that are already gone or
	// forgets who holds one.
	if _, err := sales.Reconcile(ctx, cfg.CampaignID); err != nil {
		return fmt.Errorf("reconcile stock: %w", err)
	}

	return nil
}

// run exists so that main can exit non-zero without skipping cleanup:
// os.Exit does not run deferred functions.
func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	// The correlation handler wraps the JSON one, so every line written
	// inside a request carries the id without any call site remembering to
	// add it. The lines most worth correlating are the ones written in a
	// hurry on a failure path, and those are exactly the ones that get
	// forgotten.
	logger := slog.New(correlation.NewHandler(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})))
	slog.SetDefault(logger)

	telemetry := metrics.New()

	// Signals are trapped before any dependency is opened so that a SIGTERM
	// arriving mid-startup is still honoured. A deploy that rolls back
	// while a pod is booting is exactly when this matters.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.Open(ctx, cfg.PostgresDSN, store.DefaultPoolConfig)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close()

	redis := cache.Open(cfg.RedisAddr, cfg.RedisPassword)
	defer func() {
		if err := redis.Close(); err != nil {
			logger.Warn("closing redis", slog.Any("error", err))
		}
	}()

	// The broker is the one dependency that cannot be opened lazily: AMQP has
	// no lazy dial, and a process that has not declared its topology cannot
	// know whether publishing would work. Retried rather than fatal on the
	// first attempt, because RabbitMQ's Erlang VM routinely takes half a
	// minute longer to become useful than the rest of the stack.
	broker, err := dialBroker(ctx, cfg.RabbitMQURL, logger)
	if err != nil {
		return fmt.Errorf("open rabbitmq: %w", err)
	}
	defer func() {
		if err := broker.Close(); err != nil {
			logger.Warn("closing rabbitmq", slog.Any("error", err))
		}
	}()

	publisher, err := queue.NewPublisher(broker, cfg.PublisherChannels)
	if err != nil {
		return fmt.Errorf("open publisher: %w", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			logger.Warn("closing publisher", slog.Any("error", err))
		}
	}()

	// Checked here rather than in the config package, which should not import
	// the packages it configures. Refused at startup either way: a typo in
	// this variable would silently disarm a demonstration whose whole purpose
	// is to fail, and somebody would watch a system quietly succeed and
	// conclude the recovery works.
	fault, err := purchase.ParseFault(cfg.FaultInjection)
	if err != nil {
		return fmt.Errorf("FAULT_INJECTION: %w", err)
	}
	if fault != "" {
		logger.Warn("FAULT INJECTION IS ARMED: sales will be abandoned deliberately",
			slog.String("point", string(fault)))
	}

	sales := purchase.New(redis, db, publisher, purchase.Timeouts{
		Redis:    cfg.RedisTimeout,
		Postgres: cfg.PostgresTimeout,
		Publish:  cfg.PublishTimeout,
	}, logger).
		WithObserver(telemetry).
		WithSweepPolicy(purchase.SweepPolicy{
			ReservationAge: cfg.ReservationAge,
			PendingAge:     cfg.PendingAge,
		}).
		WithFaults(purchase.Faults{
			Armed: fault,
			// A real process death, because the brief asks for one and
			// because a demonstration that politely returns an error is not
			// the thing being demonstrated. The integration test arms the
			// same point without a Kill, which stops the sale in exactly the
			// same state and leaves a process alive to assert with.
			Kill: func() {
				logger.Error("fault injection: killing the process mid-sale",
					slog.String("point", string(fault)))
				os.Exit(1)
			},
		})

	if err := prepare(ctx, cfg, db, sales, logger); err != nil {
		return err
	}

	checker := health.New(
		health.Probe{Name: "postgres", Ping: db.Ping},
		health.Probe{Name: "redis", Ping: redis.Ping},
		health.Probe{Name: "rabbitmq", Ping: broker.Ping},
	)

	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: withRequestTimeout(cfg.RequestTimeout, httpapi.New(httpapi.Config{
			Health:       checker,
			Purchaser:    sales,
			CampaignID:   cfg.CampaignID,
			Logger:       logger,
			Limiter:      redis,
			UserLimit:    httpapi.Policy{Limit: cfg.RateLimitUser, Window: cfg.RateLimitWindow},
			IPLimit:      httpapi.Policy{Limit: cfg.RateLimitIP, Window: cfg.RateLimitWindow},
			RateLimitKey: cache.RateLimitKey,
			Idempotency:  redis,
			IdempotencyPolicy: httpapi.IdempotencyPolicy{
				Lease:     cfg.IdempotencyLease,
				Retention: cfg.IdempotencyRetention,
			},
			IdempotencyKey: cache.IdempotencyKey,
			Observer:       telemetry,
			Metrics:        telemetry.Handler(),
		}).Routes()),
		// Every timeout is set explicitly. The zero value for each of
		// these is "no limit", which leaves a public listener one slow
		// client away from holding a connection open indefinitely.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// The sweeper runs beside the server rather than as its own command. It
	// needs exactly what the API already holds — Redis, PostgreSQL and a
	// publisher — and a fourth deployable to run one query a minute would be
	// three more things to configure and monitor for no benefit. Several API
	// instances is fine: the pass takes the reconciliation lock.
	sweeperDone := make(chan struct{})
	go func() {
		defer close(sweeperDone)
		runSweeper(ctx, sales, cfg, logger)
	}()

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("api listening", slog.String("addr", cfg.HTTPAddr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining in-flight requests")
	}

	// Shutdown gets a fresh context: ctx is already cancelled by the signal,
	// and reusing it would abort the very requests this is meant to drain.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}

	// The sweeper stops on the same signal, and is waited for so that a pass
	// in flight finishes rather than being cut off holding the lock.
	<-sweeperDone

	logger.Info("api stopped cleanly")
	return nil
}

// runSweeper looks for interrupted sales until the process is asked to stop.
func runSweeper(ctx context.Context, sales *purchase.Service, cfg config.Config, logger *slog.Logger) {
	ticker := time.NewTicker(cfg.SweepInterval)
	defer ticker.Stop()

	logger.Info("sweeper started",
		slog.Duration("interval", cfg.SweepInterval),
		slog.Duration("reservation_age", cfg.ReservationAge),
		slog.Duration("pending_age", cfg.PendingAge))

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// Its own budget, detached from nothing: a pass that outlives the
		// interval would otherwise overlap the next one, and both would fight
		// over the same lock while holding database connections.
		passCtx, cancel := context.WithTimeout(ctx, cfg.SweepInterval)
		result, err := sales.Sweep(passCtx, cfg.CampaignID)
		cancel()

		if err != nil && ctx.Err() == nil {
			logger.Error("sweep failed", slog.Any("error", err))
			continue
		}

		// Logged only when it did something. A line every fifteen seconds
		// saying nothing was wrong is how a log stops being read, and this is
		// a log somebody will be reading precisely when something is.
		if result.Released > 0 || result.Republished > 0 {
			logger.Warn("sweeper recovered stuck sales",
				slog.Int("released", result.Released),
				slog.Int("republished", result.Republished),
				slog.Int("closed", result.Closed))
		}
	}
}
