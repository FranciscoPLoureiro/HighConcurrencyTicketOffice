package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler does the work one message asks for.
//
// Returning nil means the work is finished and the message may be
// acknowledged. Returning an error means it should be tried again later, up to
// MaxAttempts, after which the message is parked in the dead letter queue.
type Handler func(ctx context.Context, delivery Delivery) error

// Consumer feeds a handler from the processing queue.
type Consumer struct {
	conn      *Connection
	publisher *Publisher
	logger    *slog.Logger
	prefetch  int
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
func NewConsumer(conn *Connection, publisher *Publisher, prefetch int, logger *slog.Logger) *Consumer {
	if prefetch <= 0 {
		prefetch = 1
	}
	return &Consumer{conn: conn, publisher: publisher, logger: logger, prefetch: prefetch}
}

// Consume runs the handler over the processing queue until ctx ends or the
// connection drops.
//
// It returns nil for a clean shutdown and an error for a connection that went
// away, so a caller can tell "we were asked to stop" from "we should reconnect
// and carry on".
func (c *Consumer) Consume(ctx context.Context, handler Handler) error {
	ch, err := c.conn.conn.Channel()
	if err != nil {
		return fmt.Errorf("open consume channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("set prefetch: %w", err)
	}

	deliveries, err := ch.Consume(ProcessingQueue,
		"",    // let the broker name this consumer
		false, // manual acknowledgement: the whole point is that the message
		//        survives a worker that dies halfway through
		false, // not exclusive
		false, // no-local is not supported by RabbitMQ
		false, // wait for the broker to confirm the consumer
		nil)
	if err != nil {
		return fmt.Errorf("consume %q: %w", ProcessingQueue, err)
	}

	closed := ch.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case <-ctx.Done():
			// Graceful shutdown. Cancelling the consumer tells the broker to
			// stop sending, and the deferred channel close redelivers
			// anything already in flight to another worker rather than
			// holding it until this connection times out.
			//
			// The message being worked on right now is not abandoned: the
			// caller's handler is still running on a context this does not
			// cancel, and the loop is only reached again once it returns.
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
			c.dispatch(ctx, handler, raw)
		}
	}
}

// dispatch runs one message through the handler and settles it.
func (c *Consumer) dispatch(ctx context.Context, handler Handler, raw amqp.Delivery) {
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

	if err := handler(ctx, delivery); err != nil {
		c.retry(ctx, log, delivery, err)
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

// retry sends a failed message to its next attempt, or gives up on it.
func (c *Consumer) retry(ctx context.Context, log *slog.Logger, delivery Delivery, cause error) {
	// A detached context, because the usual reason to be here during a
	// shutdown is that the handler was cut short by it — and re-queueing the
	// message is exactly what must still happen in that case.
	ctx = context.WithoutCancel(ctx)

	if delivery.Attempt >= MaxAttempts {
		log.Error("message failed every attempt and is going to the dead letter queue",
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
