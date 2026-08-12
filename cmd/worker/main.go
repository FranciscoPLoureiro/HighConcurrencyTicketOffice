// Command worker fulfils tickets the API has already sold.
//
// A separate process rather than a goroutine inside the API, because the two
// scale on different things. The API is bound by a burst that lasts seconds;
// the worker is bound by however long a document takes to draw, and wanting
// more of one is not a reason to run more of the other.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/config"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/fulfilment"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/queue"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker exited with an error", slog.Any("error", err))
		os.Exit(1)
	}
}

// brokerDialBudget bounds how long startup waits for RabbitMQ, and
// reconnectDelay how long it waits before redialling one that dropped.
//
// The delay is not zero, because the usual reason a connection drops is that
// the broker is restarting, and a redial loop with no pause turns that into a
// few thousand refused connections per second against something already busy
// coming back.
const (
	brokerDialBudget = 90 * time.Second
	reconnectDelay   = 2 * time.Second
)

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(logger)

	// Trapped before any dependency is opened, so that a SIGTERM arriving
	// while the broker is still coming up is honoured rather than ignored
	// for the ninety seconds the dial is allowed to take.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.Open(ctx, cfg.PostgresDSN, workerPoolConfig())
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close()

	// No migrations and no reconciliation here. Both belong to the API,
	// which owns the schema and the stock; a worker that also migrated would
	// be a second writer racing the first through the same advisory lock for
	// no benefit, and one that reconciled would be rebuilding Redis from
	// PostgreSQL while the API is selling from it.
	work := fulfilment.New(db, cfg.FulfilmentDelay, cfg.PostgresTimeout, logger)

	logger.Info("worker starting",
		slog.Int("prefetch", cfg.WorkerPrefetch),
		slog.Duration("fulfilment_delay", cfg.FulfilmentDelay))

	if err := serve(ctx, cfg, work.Fulfil, logger); err != nil {
		return err
	}

	logger.Info("worker stopped cleanly")
	return nil
}

// workerPoolConfig sizes the worker's connection pool.
//
// Smaller than the API's, and for a reason worth stating: this process is not
// contended. It holds at most WorkerPrefetch messages at once and each does one
// short write, so anything above that number is connections that exist to be
// idle. The API's pool absorbs a burst; this one absorbs nothing.
func workerPoolConfig() store.PoolConfig {
	cfg := store.DefaultPoolConfig
	cfg.MaxConns = 8
	cfg.MinConns = 2
	return cfg
}

// serve consumes until asked to stop, redialling a connection that drops.
//
// The loop is the reconnection strategy. A worker whose broker restarted and
// which never came back is indistinguishable from a healthy one with an empty
// queue, and the tickets it should be fulfilling simply age in place — so
// Consume reporting a dropped connection is a reason to dial again, and only a
// cancelled context is a reason to stop.
func serve(ctx context.Context, cfg config.Config, handler queue.Handler, logger *slog.Logger) error {
	for {
		err := consumeOnce(ctx, cfg, handler, logger)

		// Checked before the error, not after. Shutting down closes the
		// connection underneath the consumer, so the last thing it reports on
		// the way out is usually a failure — and reporting that as one would
		// make every clean deploy exit non-zero.
		//
		// nilerr sees a non-nil error discarded and objects, which is a good
		// rule and the wrong one here: the error describes a connection that
		// was closed on purpose, by us, one line after we were told to stop.
		if ctx.Err() != nil {
			return nil //nolint:nilerr // a cancelled context is a clean stop, not a failure
		}
		if err == nil {
			return nil
		}

		logger.Error("consumer stopped, reconnecting", slog.Any("error", err))

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(reconnectDelay):
		}
	}
}

// consumeOnce runs one connection's worth of consuming.
func consumeOnce(ctx context.Context, cfg config.Config, handler queue.Handler, logger *slog.Logger) error {
	broker, err := dialBroker(ctx, cfg.RabbitMQURL, logger)
	if err != nil {
		return fmt.Errorf("open rabbitmq: %w", err)
	}
	defer func() {
		if err := broker.Close(); err != nil {
			logger.Warn("closing rabbitmq", slog.Any("error", err))
		}
	}()

	// The consumer needs a publisher of its own: a retry is a publish to a
	// waiting room, and giving up on a message is a publish to the dead
	// letter queue. Two channels is plenty for a process that publishes only
	// when something has gone wrong.
	publisher, err := queue.NewPublisher(broker, 2)
	if err != nil {
		return fmt.Errorf("open publisher: %w", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			logger.Warn("closing publisher", slog.Any("error", err))
		}
	}()

	// The work budget covers the pretend render plus the write that follows
	// it, with room to spare. Too tight and a slow database turns every
	// message into a retry; too loose and a wedged dependency holds every
	// prefetched slot until somebody notices.
	workTimeout := cfg.FulfilmentDelay + cfg.PostgresTimeout + 5*time.Second

	consumer := queue.NewConsumer(broker, publisher, cfg.WorkerPrefetch, workTimeout, logger)

	logger.Info("worker consuming", slog.String("queue", queue.ProcessingQueue))
	return consumer.Consume(ctx, handler)
}

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
		case <-time.After(reconnectDelay):
		}
	}
}
