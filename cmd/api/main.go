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
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/health"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/httpapi"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/purchase"
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

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(logger)

	// Signals are trapped before any dependency is opened so that a SIGTERM
	// arriving mid-startup is still honoured. A deploy that rolls back
	// while a pod is booting is exactly when this matters.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.Open(ctx, cfg.PostgresDSN)
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

	sales := purchase.New(redis, db, logger)

	if err := prepare(ctx, cfg, db, sales, logger); err != nil {
		return err
	}

	checker := health.New(
		health.Probe{Name: "postgres", Ping: db.Ping},
		health.Probe{Name: "redis", Ping: redis.Ping},
	)

	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: httpapi.New(httpapi.Config{
			Health:       checker,
			Purchaser:    sales,
			CampaignID:   cfg.CampaignID,
			Logger:       logger,
			Limiter:      redis,
			UserLimit:    httpapi.Policy{Limit: cfg.RateLimitUser, Window: cfg.RateLimitWindow},
			IPLimit:      httpapi.Policy{Limit: cfg.RateLimitIP, Window: cfg.RateLimitWindow},
			RateLimitKey: cache.RateLimitKey,
		}).Routes(),
		// Every timeout is set explicitly. The zero value for each of
		// these is "no limit", which leaves a public listener one slow
		// client away from holding a connection open indefinitely.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

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

	logger.Info("api stopped cleanly")
	return nil
}
