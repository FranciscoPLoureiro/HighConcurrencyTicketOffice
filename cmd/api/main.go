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
