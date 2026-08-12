// Command worker fulfils tickets the API has already sold.
//
// A separate process rather than a goroutine inside the API, because the two
// scale on different things. The API is bound by a burst that lasts seconds;
// the worker is bound by however long a document takes to draw, and wanting
// more of one is not a reason to run more of the other.
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
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/compensation"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/config"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/correlation"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/fulfilment"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/metrics"
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

	// Wrapped exactly as the API wraps it, which is the point: an id minted
	// for an HTTP request travels in the queue message and comes back out
	// here, so one search spans both processes.
	logger := slog.New(correlation.NewHandler(
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})))
	slog.SetDefault(logger)

	telemetry := metrics.New()

	// Trapped before any dependency is opened, so that a SIGTERM arriving
	// while the broker is still coming up is honoured rather than ignored
	// for the ninety seconds the dial is allowed to take.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// A listener that serves nothing but /metrics. The worker answers no
	// requests and wants no port for its own sake, but a process Prometheus
	// cannot reach is one whose queue depth, duplicate rate and fulfilment
	// latency exist only in its own memory.
	metricsServer := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           metricsRoutes(telemetry),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		logger.Info("worker metrics listening", slog.String("addr", cfg.MetricsAddr))
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// Not fatal, deliberately. A worker that cannot be scraped should
			// carry on fulfilling tickets: losing the metrics is bad, and
			// stopping the work because of it is worse.
			logger.Error("worker metrics listener stopped", slog.Any("error", err))
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			logger.Warn("closing the metrics listener", slog.Any("error", err))
		}
	}()

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
	// The worker needs Redis now, which it did not before: reversing a sale
	// means putting its ticket back on the shelf, and the shelf is in Redis.
	redis := cache.Open(cfg.RedisAddr, cfg.RedisPassword)
	defer func() {
		if err := redis.Close(); err != nil {
			logger.Warn("closing redis", slog.Any("error", err))
		}
	}()

	work := fulfilment.New(db, cfg.FulfilmentDelay, cfg.PostgresTimeout, logger).
		WithObserver(telemetry)

	logger.Info("worker starting",
		slog.Int("prefetch", cfg.WorkerPrefetch),
		slog.Duration("fulfilment_delay", cfg.FulfilmentDelay))

	if err := serve(ctx, cfg, work.Fulfil, db, redis, telemetry, logger); err != nil {
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
func serve(ctx context.Context, cfg config.Config, handler queue.Handler,
	db *store.Store, redis *cache.Cache, telemetry *metrics.Metrics, logger *slog.Logger,
) error {
	for {
		err := consumeOnce(ctx, cfg, handler, db, redis, telemetry, logger)

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
func consumeOnce(ctx context.Context, cfg config.Config, handler queue.Handler,
	db *store.Store, redis *cache.Cache, telemetry *metrics.Metrics, logger *slog.Logger,
) error {
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

	// Depth is polled rather than inferred from the messages this worker
	// happens to see, because the number that matters is the backlog nobody
	// has started on. It is also the only honest liveness signal a process
	// serving nothing can offer: a worker wedged on a dependency looks
	// perfectly alive from outside and shows up here within one tick.
	depthCtx, stopPolling := context.WithCancel(ctx)
	defer stopPolling()
	go pollQueueDepth(depthCtx, broker, telemetry, cfg.QueueDepthInterval, logger)

	// The compensation saga drains the dead letter queue alongside the main
	// consumer. In the same process because it needs the same three
	// connections and runs approximately never; in its own goroutine because
	// a worker busy fulfilling tickets must not be the reason a failed sale
	// keeps its seat.
	saga := compensation.New(db, redis, publisher, cfg.PostgresTimeout, logger).
		WithObserver(telemetry)

	compensator := queue.NewConsumer(broker, publisher, 1, workTimeout, logger)

	sagaDone := make(chan struct{})
	go func() {
		defer close(sagaDone)
		// SettleByReturning, not the retry tiers. These messages are already
		// in the dead letter queue; sending one through the waiting rooms
		// would dead-letter it back onto the *processing* queue and hand a
		// message known not to work to the workers all over again.
		if err := compensator.ConsumeFrom(ctx, queue.DeadLetterQueue,
			queue.SettleByReturning, saga.Compensate); err != nil && ctx.Err() == nil {
			logger.Error("compensation consumer stopped", slog.Any("error", err))
		}
	}()

	logger.Info("worker consuming",
		slog.String("queue", queue.ProcessingQueue),
		slog.String("compensating", queue.DeadLetterQueue))

	err = consumer.Consume(ctx, handler)
	<-sagaDone
	return err
}

// metricsRoutes exposes the registry and nothing else.
func metricsRoutes(telemetry *metrics.Metrics) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", telemetry.Handler())
	return mux
}

// pollQueueDepth records how deep each queue is, until told to stop.
//
// The retry tiers are watched alongside the processing queue on purpose. A
// backlog on the first means the workers are behind; a backlog on a retry queue
// means something is failing over and over. Summed into one number those two
// are indistinguishable, and they call for opposite responses — more workers,
// or fewer until somebody has looked.
func pollQueueDepth(ctx context.Context, broker *queue.Connection, telemetry *metrics.Metrics,
	every time.Duration, logger *slog.Logger,
) {
	watched := append([]string{queue.ProcessingQueue, queue.DeadLetterQueue}, queue.RetryQueues()...)

	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		for _, name := range watched {
			depth, err := broker.QueueDepth(ctx, name)
			if err != nil {
				// Expected while a connection is going away, and not worth a
				// line per queue per tick on the way out.
				if ctx.Err() == nil {
					logger.Warn("could not read queue depth",
						slog.String("queue", name),
						slog.Any("error", err))
				}
				continue
			}
			telemetry.SetQueueDepth(name, depth)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
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
