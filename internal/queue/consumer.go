package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler does the work one message asks for.
//
// Returning nil means the work is finished and the message may be
// acknowledged. Returning an error means it should be tried again later, up to
// MaxAttempts, after which the message is parked in the dead letter queue.
//
// Returning an error wrapping ErrPermanent skips the retries entirely.
type Handler func(ctx context.Context, delivery Delivery) error

// ErrPermanent marks a failure that will not come right on its own.
//
// A message referring to a purchase that does not exist, or one that has
// already been cancelled, fails identically on every attempt — so retrying it
// is a delay with no chance of success, and the two waits it spends doing so
// are two waits nobody gets to see the real problem in. It goes straight to the
// dead letter queue.
var ErrPermanent = errors.New("failure will not resolve by retrying")

// returnPause is how long a consumer waits after handing a message back.
//
// Without it, a dependency that is down turns the dead letter consumer into a
// hot loop redelivering the same message thousands of times a second against
// something already struggling.
const returnPause = 2 * time.Second

// Consumer feeds a handler from the processing queue.
type Consumer struct {
	conn        *Connection
	publisher   *Publisher
	logger      *slog.Logger
	prefetch    int
	workTimeout time.Duration
}

// NewConsumer builds a consumer.
//
// The prefetch is how many unacknowledged messages the broker will send before
// waiting. It is the one number that decides how a worker behaves under a
// backlog. Left at the default of zero — unlimited — one worker takes the
// entire queue into memory the moment it connects, and a second worker starting
// afterwards has nothing to do while the first is still two hundred messages
// behind. Small numbers spread work across workers; larger ones cut the round
// trips. Fulfilment here takes seconds, so the round trip is noise and the
// spreading is the whole benefit.
//
// The work timeout bounds one handler run. It is the only thing standing
// between a dependency that stops answering and a worker whose every prefetched
// slot is occupied forever by a message nobody is making progress on.
func NewConsumer(conn *Connection, publisher *Publisher, prefetch int, workTimeout time.Duration, logger *slog.Logger) *Consumer {
	if prefetch <= 0 {
		prefetch = 1
	}
	if workTimeout <= 0 {
		workTimeout = time.Minute
	}
	return &Consumer{
		conn:        conn,
		publisher:   publisher,
		logger:      logger,
		prefetch:    prefetch,
		workTimeout: workTimeout,
	}
}

// Settlement is what happens to a message the handler could not finish.
type Settlement int

const (
	// SettleWithRetry sends a failure through the waiting rooms and then to
	// the dead letter queue. For work that should succeed eventually.
	SettleWithRetry Settlement = iota

	// SettleByReturning puts a failure back on the queue it came from.
	//
	// For a consumer that is already draining the dead letter queue, where
	// there is nowhere further to send anything: retrying into the tiers
	// would dead-letter it back onto the *processing* queue, handing a
	// message already known not to work to the workers all over again.
	SettleByReturning
)

// Consume runs the handler over the processing queue until ctx ends or the
// connection drops.
//
// It returns nil for a clean shutdown and an error for a connection that went
// away, so a caller can tell "we were asked to stop" from "we should reconnect
// and carry on".
func (c *Consumer) Consume(ctx context.Context, handler Handler) error {
	return c.ConsumeFrom(ctx, ProcessingQueue, SettleWithRetry, handler)
}

// ConsumeFrom runs the handler over one named queue.
func (c *Consumer) ConsumeFrom(ctx context.Context, queue string, settlement Settlement, handler Handler) error {
	ch, err := c.conn.conn.Channel()
	if err != nil {
		return fmt.Errorf("open consume channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("set prefetch: %w", err)
	}

	deliveries, err := ch.Consume(queue,
		"",    // let the broker name this consumer
		false, // manual acknowledgement: the whole point is that the message
		//        survives a worker that dies halfway through
		false, // not exclusive
		false, // no-local is not supported by RabbitMQ
		false, // wait for the broker to confirm the consumer
		nil)
	if err != nil {
		return fmt.Errorf("consume %q: %w", queue, err)
	}

	closed := ch.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case <-ctx.Done():
			// Graceful shutdown. Closing the channel — deferred above —
			// tells the broker to stop sending and redelivers anything it
			// had already pushed to another worker, rather than holding it
			// until this connection times out.
			//
			// Nothing is interrupted mid-flight: dispatch runs inline, so
			// reaching this case at all means no message is being worked on.
			// The one that was is finished, settled and acknowledged.
			return nil

		case err := <-closed:
			if err == nil {
				return nil
			}
			return fmt.Errorf("consume channel closed: %w", err)

		case raw, ok := <-deliveries:
			if !ok {
				return errors.New("consume channel closed without notice")
			}
			c.dispatch(ctx, handler, settlement, raw)
		}
	}
}

// dispatch runs one message through the handler and settles it.
func (c *Consumer) dispatch(ctx context.Context, handler Handler, settlement Settlement, raw amqp.Delivery) {
	delivery, err := readDelivery(raw)
	if err != nil {
		// A message this build cannot parse will not parse any better on the
		// next attempt, so retrying it is a loop with extra steps. It goes
		// straight to the dead letter queue, where a person can look at it.
		c.logger.Error("unreadable message sent straight to the dead letter queue",
			slog.Any("error", err))
		c.reject(raw)
		return
	}

	log := c.logger.With(
		slog.String("purchase_id", delivery.Message.PurchaseID),
		slog.String("correlation_id", delivery.Message.CorrelationID),
		slog.Int("attempt", delivery.Attempt))

	// The handler's context is deliberately detached from the one that
	// signals shutdown, and bounded by its own budget instead.
	//
	// The brief asks that a worker finish the message it is holding before it
	// closes, and inheriting the cancellation would do the opposite: SIGTERM
	// would abort the fulfilment halfway, and every deploy would leave a
	// scattering of tickets to be retried. Stopping means taking no more
	// work, not dropping the work in hand. What bounds the wait is the
	// process's own shutdown timeout, which is a decision for the caller.
	workCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.workTimeout)
	defer cancel()

	if err := handler(workCtx, delivery); err != nil {
		if settlement == SettleByReturning {
			// Back where it came from, and a pause before the next message.
			// Without one, a dependency that is down turns this into a hot
			// loop redelivering the same message thousands of times a second
			// against something already struggling.
			log.Error("could not settle a message, returning it to its queue",
				slog.Any("error", err))
			if nackErr := delivery.raw.Nack(false, true); nackErr != nil {
				log.Error("could not return the message either", slog.Any("error", nackErr))
			}
			select {
			case <-ctx.Done():
			case <-time.After(returnPause):
			}
			return
		}

		c.settleFailure(ctx, log, delivery, err)
		return
	}

	// Acknowledged only now, after the work is finished and durably recorded.
	// Acknowledging first would turn every crash between the two into a
	// ticket that was taken off the queue and never fulfilled — which is the
	// failure this whole design exists to avoid, reintroduced one line
	// earlier than usual.
	if err := raw.Ack(false); err != nil {
		// The work is done and the broker did not hear us say so, which
		// means it will send the message again. That is safe because the
		// handler is idempotent, and it is worth a line because a burst of
		// these is a sick connection rather than a sick handler.
		log.Warn("could not acknowledge a finished message; it will be redelivered",
			slog.Any("error", err))
	}
}

// settleFailure sends a failed message to its next attempt, or gives up on it.
func (c *Consumer) settleFailure(ctx context.Context, log *slog.Logger, delivery Delivery, cause error) {
	// A detached context, because the usual reason to be here during a
	// shutdown is that the handler was cut short by it — and re-queueing the
	// message is exactly what must still happen in that case.
	ctx = context.WithoutCancel(ctx)

	permanent := errors.Is(cause, ErrPermanent)
	if permanent || delivery.Attempt >= MaxAttempts {
		log.Error("message is going to the dead letter queue",
			slog.Bool("permanent", permanent),
			slog.Any("error", cause))

		if err := c.publisher.DeadLetter(ctx, delivery.Message, delivery.Attempt); err != nil {
			// Nacking without requeue is the fallback: the message is
			// dropped here, and the purchase is left pending for the phase 5
			// sweeper rather than spinning forever.
			log.Error("could not park a failed message", slog.Any("error", err))
			c.reject(delivery.raw)
			return
		}
		c.ack(log, delivery.raw)
		return
	}

	next := delivery.Attempt + 1
	log.Warn("message failed and will be retried", slog.Int("next_attempt", next), slog.Any("error", cause))

	if err := c.publisher.Retry(ctx, delivery.Message, next); err != nil {
		// The retry could not be published, so the only way to keep the
		// message is to hand it back to the broker. It comes round again
		// immediately and with the same attempt number, which is worse than
		// the delayed retry and much better than losing it.
		log.Error("could not schedule a retry, returning the message to the queue",
			slog.Any("error", err))
		if nackErr := delivery.raw.Nack(false, true); nackErr != nil {
			log.Error("could not return the message either", slog.Any("error", nackErr))
		}
		return
	}

	// The retry is durably published, so this copy has been dealt with.
	//
	// Publishing and then acknowledging is two writes that are not atomic,
	// and a crash between them means the message is both scheduled and
	// redelivered. That is a duplicate, not a loss, and a duplicate is what
	// the handler is built to survive — which is the trade at-least-once
	// delivery asks everyone to make.
	c.ack(log, delivery.raw)
}

func (c *Consumer) ack(log *slog.Logger, raw amqp.Delivery) {
	if err := raw.Ack(false); err != nil {
		log.Warn("could not acknowledge a settled message", slog.Any("error", err))
	}
}

// reject drops a message without requeueing it.
func (c *Consumer) reject(raw amqp.Delivery) {
	if err := raw.Nack(false, false); err != nil {
		c.logger.Error("could not reject a message", slog.Any("error", err))
	}
}
