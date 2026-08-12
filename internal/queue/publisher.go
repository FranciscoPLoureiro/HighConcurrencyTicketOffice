package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// The three ways a publish fails, split by what a caller may conclude from
// them. The distinction is the whole reason they are separate errors: undoing
// a sale on a failure that did not actually happen is how a ticket is sold
// twice, and refusing to undo one on a failure that did is how a ticket is lost.
var (
	// ErrPublishRefused means the broker nacked the message. It is
	// definitive: the broker considered the message and declined to take
	// responsibility, usually because it is out of disk or memory. Nothing
	// holds the message, and a caller may safely undo what it published
	// about.
	ErrPublishRefused = errors.New("broker refused the publish")

	// ErrPublishUnconfirmed means no answer arrived before the deadline. It
	// is the ambiguous one. The broker may have taken the message and been
	// slow to say so, so a caller must not conclude the message is gone —
	// undoing here can undo a sale that is about to be fulfilled anyway.
	ErrPublishUnconfirmed = errors.New("broker did not confirm the publish in time")

	// ErrUnroutable means the broker accepted the message and had nowhere to
	// put it. Definitive, and invisible without mandatory publishing: the
	// broker acknowledges an unroutable message perfectly happily, having
	// discarded it.
	ErrUnroutable = errors.New("no queue is bound for the message")
)

// Publisher sends messages and waits for the broker to say it has them.
//
// Publisher confirms are the whole point of this type. Without them a publish
// is a write to a socket: it succeeds locally, the broker may be out of disk or
// mid-failover, and the ticket disappears with nothing anywhere reporting a
// problem. With them, a publish that returns nil means the broker has accepted
// responsibility for the message.
//
// Channels are pooled because amqp091 channels are not safe for concurrent use
// and a confirmed publish is a round trip. One shared channel would serialise
// every sale behind the broker's acknowledgement latency; a pool bounds the
// concurrency without inventing a channel per request, which the broker would
// charge for in memory.
type Publisher struct {
	channels chan *confirmChannel
	closed   sync.Once
}

// confirmChannel is one pooled channel and the returns the broker sent back on
// it.
type confirmChannel struct {
	ch      *amqp.Channel
	returns chan amqp.Return
}

// NewPublisher builds a pool of confirming channels.
func NewPublisher(conn *Connection, size int) (*Publisher, error) {
	if size <= 0 {
		size = 1
	}

	p := &Publisher{channels: make(chan *confirmChannel, size)}

	for range size {
		ch, err := conn.conn.Channel()
		if err != nil {
			_ = p.Close()
			return nil, fmt.Errorf("open publish channel: %w", err)
		}
		// Confirm mode is per channel and has to be asked for. A channel
		// without it silently reports every publish as successful.
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			_ = p.Close()
			return nil, fmt.Errorf("put channel into confirm mode: %w", err)
		}

		// Buffered so the broker's return never blocks on nobody reading:
		// an unbuffered NotifyReturn channel stalls the connection's reader
		// goroutine, which stops confirms arriving as well.
		returns := make(chan amqp.Return, size)
		ch.NotifyReturn(returns)

		p.channels <- &confirmChannel{ch: ch, returns: returns}
	}

	return p, nil
}

// PublishTicket sends one ticket for fulfilment and waits for the confirm.
func (p *Publisher) PublishTicket(ctx context.Context, message TicketMessage) error {
	return p.publish(ctx, ProcessKey, message, 1)
}

// Retry sends a message back for another attempt after the tier's delay.
//
// The message goes to a waiting room, not back onto the processing queue. A
// straight requeue would be redelivered immediately, and a dependency that is
// down stays down for longer than the round trip it takes to fail again — so an
// immediate retry is a busy loop against something already struggling.
func (p *Publisher) Retry(ctx context.Context, message TicketMessage, nextAttempt int) error {
	tier := nextAttempt - 2
	if tier < 0 {
		tier = 0
	}
	if tier >= len(retryTiers) {
		tier = len(retryTiers) - 1
	}

	return p.publish(ctx, retryTiers[tier].RoutingKey, message, nextAttempt)
}

// Compensate records that a sale was reversed, for whatever wants to know.
//
// Published after the reversal rather than before it. If this fails the ticket
// is still back on the shelf and the row is still cancelled, which is the part
// that matters; a lost notification is worth a log line and not worth undoing
// a correct compensation over.
func (p *Publisher) Compensate(ctx context.Context, message TicketMessage) error {
	return p.publish(ctx, CompensationKey, message, MaxAttempts)
}

// DeadLetter parks a message that has failed every attempt.
func (p *Publisher) DeadLetter(ctx context.Context, message TicketMessage, attempt int) error {
	return p.publish(ctx, DeadKey, message, attempt)
}

func (p *Publisher) publish(ctx context.Context, routingKey string, message TicketMessage, attempt int) error {
	frame, err := message.publishing(attempt)
	if err != nil {
		return err
	}

	channel, err := p.take(ctx)
	if err != nil {
		return err
	}
	defer func() { p.channels <- channel }()

	// Anything the broker returned about an earlier publish on this channel
	// is stale by now and would be misread as this message coming back.
	drain(channel.returns)

	confirmation, err := channel.ch.PublishWithDeferredConfirmWithContext(ctx,
		Exchange, routingKey,
		// mandatory: tell us rather than silently dropping a message with
		// nowhere to go. A confirm alone does not cover this — the broker
		// acknowledges an unroutable message perfectly happily, having
		// thrown it away.
		true,
		false, // immediate is not supported by modern brokers
		frame)
	if err != nil {
		return fmt.Errorf("publish to %q: %w", routingKey, err)
	}

	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPublishUnconfirmed, err)
	}
	if !acked {
		return fmt.Errorf("%w: %s", ErrPublishRefused, routingKey)
	}

	// The return, if there is one, always precedes the acknowledgement on
	// the same channel, so by here it has arrived or it never will.
	select {
	case returned := <-channel.returns:
		if returned.MessageId == message.PurchaseID {
			return fmt.Errorf("%w: %s returned %d %s",
				ErrUnroutable, routingKey, returned.ReplyCode, returned.ReplyText)
		}
	default:
	}

	return nil
}

// take borrows a channel, waiting if every one of them is busy.
func (p *Publisher) take(ctx context.Context) (*confirmChannel, error) {
	select {
	case channel := <-p.channels:
		return channel, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("wait for a publish channel: %w", ctx.Err())
	}
}

func drain(returns chan amqp.Return) {
	for {
		select {
		case <-returns:
		default:
			return
		}
	}
}

// Close releases every pooled channel.
//
// It takes what is in the pool and does not wait for what is not. A channel
// still out on a publish is closed with the connection a moment later, and
// blocking here to collect it would mean shutdown hangs on whichever publish is
// slowest — which is the request graceful shutdown is already bounding
// elsewhere. The Go channel is deliberately not closed: a publish returning its
// borrowed channel to a closed one would panic on the way out.
func (p *Publisher) Close() error {
	var err error
	p.closed.Do(func() {
		for range cap(p.channels) {
			select {
			case channel := <-p.channels:
				if closeErr := channel.ch.Close(); closeErr != nil && !errors.Is(closeErr, amqp.ErrClosed) {
					err = errors.Join(err, closeErr)
				}
			default:
				return
			}
		}
	})
	return err
}
