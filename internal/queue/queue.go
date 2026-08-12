// Package queue owns access to RabbitMQ.
//
// It carries one kind of message — a ticket waiting to be fulfilled — from the
// API that sold it to the worker that finishes it. What makes it worth a
// package of its own is not the plumbing but the two guarantees around it: a
// publish is not reported as successful until the broker says it took
// responsibility, and a message that keeps failing ends up somewhere a person
// can look at it rather than in a loop nobody notices.
//
// Delivery is at-least-once, and no configuration turns that into exactly-once.
// The consumer is expected to be idempotent; see the README for why that is the
// only honest arrangement.
package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// The topology, declared by both processes at startup.
//
// Both, rather than by whichever happens to start first, because a queue that
// exists only if the API booted before the worker is a deployment order nobody
// wrote down. Declaring is idempotent as long as the arguments match, and the
// arguments are all here.
const (
	// Exchange is direct rather than topic: there is one kind of message and
	// three places it can go, and routing keys that mean exactly what they
	// say are easier to reason about than patterns that could match more.
	Exchange = "tickets"

	// ProcessingQueue holds tickets waiting for their document.
	ProcessingQueue = "ticket_processing_queue"
	// DeadLetterQueue holds messages that failed every attempt. Nothing
	// consumes it: its purpose is to stop and be looked at.
	DeadLetterQueue = "ticket_dead_letter_queue"

	// The routing keys.
	ProcessKey = "ticket.process"
	DeadKey    = "ticket.dead"
)

// retryTiers are the waiting rooms a failed message passes through.
//
// Each is an ordinary queue with a message TTL and no consumer, dead-lettering
// back onto the processing queue when the TTL expires. RabbitMQ has no delayed
// delivery without a plugin, and this is the standard way to get it out of the
// primitives that are always there.
//
// One queue per delay rather than one queue with per-message TTLs. A single
// queue expires messages strictly in publication order, so a message with a
// thirty second wait parked at the head holds up every five second message
// behind it — the delay becomes whatever the slowest neighbour asked for. Two
// queues cost two declarations and remove the problem entirely.
//
// The delays grow because the failures worth retrying rarely clear instantly. A
// database failing over, a broker being restarted and a dependency briefly out
// of memory all take longer than five seconds, and hammering them meanwhile is
// how a small outage becomes a large one.
var retryTiers = []struct {
	Queue      string
	RoutingKey string
	Delay      time.Duration
}{
	{"ticket_retry_5s", "ticket.retry.5s", 5 * time.Second},
	{"ticket_retry_30s", "ticket.retry.30s", 30 * time.Second},
}

// MaxAttempts is how many times a message is delivered to the worker before it
// is given up on.
//
// Three, which is what the brief asks for and also the number that matches the
// retry tiers: an attempt, a five second wait, an attempt, a thirty second
// wait, a last attempt. Beyond that a failure is not transient, and the useful
// thing to do with it is stop and be visible.
const MaxAttempts = 3

// attemptHeader counts deliveries to the worker.
//
// Carried explicitly rather than read out of RabbitMQ's own x-death header.
// x-death counts dead-letterings per queue, so with two retry tiers it holds
// two entries that each say "1" and the number this code actually wants is not
// in there without summing them and knowing which queues to sum. An integer we
// set ourselves means the same thing in every version of the broker.
const attemptHeader = "x-attempt"

// Connection is a live connection to the broker and the topology it serves.
type Connection struct {
	conn *amqp.Connection
}

// Dial opens a connection and declares everything this package needs.
//
// Unlike the Postgres pool and the Redis client, this connects eagerly: AMQP
// has no lazy dial, and a process that has not declared its topology cannot
// know whether it would work. The caller is expected to retry — see the retry
// loop in both commands — so a broker that is thirty seconds behind the rest of
// the stack is a slow start rather than a crash loop.
func Dial(ctx context.Context, url string) (*Connection, error) {
	conn, err := amqp.DialConfig(url, amqp.Config{
		// Heartbeats are how a peer that vanished without closing the socket
		// is noticed. Without them a worker can sit holding an unacked
		// message against a broker that is no longer there, and the message
		// is neither being processed nor redelivered to anyone else.
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
	})
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}

	c := &Connection{conn: conn}
	if err := c.declare(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return c, nil
}

// declare creates the exchange, the queues and the bindings.
func (c *Connection) declare(_ context.Context) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("open declaration channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.ExchangeDeclare(Exchange, amqp.ExchangeDirect,
		true,  // durable: the exchange outlives a broker restart
		false, // not auto-deleted: it exists whether or not anything is bound
		false, // not internal
		false, // wait for the broker to confirm
		nil); err != nil {
		return fmt.Errorf("declare exchange %q: %w", Exchange, err)
	}

	// Every queue is durable and every message is persistent. A broker
	// restart between a sale and its fulfilment would otherwise lose the
	// ticket, and the row in PostgreSQL would sit pending forever with
	// nothing left to act on it.
	queues := []struct {
		name string
		key  string
		args amqp.Table
	}{
		{ProcessingQueue, ProcessKey, nil},
		{DeadLetterQueue, DeadKey, nil},
	}
	for _, tier := range retryTiers {
		queues = append(queues, struct {
			name string
			key  string
			args amqp.Table
		}{
			name: tier.Queue,
			key:  tier.RoutingKey,
			args: amqp.Table{
				// The wait itself. Nothing consumes these queues, so a
				// message sits here until the TTL expires and the broker
				// dead-letters it back onto the processing queue.
				"x-message-ttl":             tier.Delay.Milliseconds(),
				"x-dead-letter-exchange":    Exchange,
				"x-dead-letter-routing-key": ProcessKey,
			},
		})
	}

	for _, q := range queues {
		if _, err := ch.QueueDeclare(q.name,
			true,  // durable
			false, // not auto-deleted when the last consumer goes away
			false, // not exclusive: both processes and every replica share it
			false, // wait for the broker to confirm
			q.args); err != nil {
			return fmt.Errorf("declare queue %q: %w", q.name, err)
		}
		if err := ch.QueueBind(q.name, q.key, Exchange, false, nil); err != nil {
			return fmt.Errorf("bind queue %q: %w", q.name, err)
		}
	}

	return nil
}

// Ping reports whether the connection is still usable.
//
// Opening and closing a channel rather than reading a flag: amqp091 marks a
// connection closed only once it has noticed, and the point of a health check
// is to notice.
func (c *Connection) Ping(_ context.Context) error {
	if c.conn.IsClosed() {
		return errors.New("rabbitmq connection is closed")
	}

	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	return ch.Close()
}

// QueueDepth reports how many messages are waiting in a queue.
//
// A passive declare rather than the management HTTP API: it needs no second
// credential, no second port and no second client, and it fails cleanly if the
// queue is missing instead of inventing one.
//
// Worth having beyond the tests that use it. Queue depth is the honest health
// signal for a worker — a process that is running proves nothing, and a backlog
// that stops moving proves quite a lot — and it is the number phase 4 puts on a
// dashboard.
func (c *Connection) QueueDepth(_ context.Context, name string) (int, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return 0, fmt.Errorf("open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	state, err := ch.QueueDeclarePassive(name, true, false, false, false, nil)
	if err != nil {
		return 0, fmt.Errorf("inspect queue %q: %w", name, err)
	}
	return state.Messages, nil
}

// Closed reports a channel that is signalled when the connection drops.
//
// The worker uses it to notice that its consumer has stopped being fed, which
// is not otherwise distinguishable from a quiet queue.
func (c *Connection) Closed() chan *amqp.Error {
	return c.conn.NotifyClose(make(chan *amqp.Error, 1))
}

// Close shuts the connection down.
//
// Closing something the broker has already closed is not an error worth
// reporting: it is what a connection that dropped underneath us looks like, and
// the caller is on its way out anyway.
func (c *Connection) Close() error {
	if err := c.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		return fmt.Errorf("close rabbitmq connection: %w", err)
	}
	return nil
}
