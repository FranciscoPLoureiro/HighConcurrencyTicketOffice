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
	"syscall"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/cache"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/config"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/health"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/httpapi"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("api exited with an error", slog.Any("error", err))
		os.Exit(1)
	}
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

	checker := health.New(
		health.Probe{Name: "postgres", Ping: db.Ping},
		health.Probe{Name: "redis", Ping: redis.Ping},
	)

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: httpapi.New(checker, logger).Routes(),
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
