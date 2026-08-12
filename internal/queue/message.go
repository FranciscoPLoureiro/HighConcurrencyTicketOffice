package queue

import (
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TicketMessage is one ticket waiting for its document.
//
// It carries identifiers and no state. The row in PostgreSQL is the state, and
// a message that repeated it would be a second copy of the truth that can
// disagree with the first — which is exactly the class of bug this project is
// about. The worker reads what it needs from the source of truth.
type TicketMessage struct {
	// PurchaseID is what the worker settles and what the client polls.
	PurchaseID string `json:"purchase_id"`
	CampaignID string `json:"campaign_id"`
	UserID     string `json:"user_id"`
	// IdempotencyKey is the client's key for the request that made this
	// purchase. It is here so that the worker's own idempotency, and the
	// compensation in phase 5, can be tied back to the request a person
	// actually made rather than only to a row.
	IdempotencyKey string `json:"idempotency_key"`
	// CorrelationID follows one request from the API's log lines to the
	// worker's. Without it the two processes produce two unrelated stories
	// about the same ticket.
	CorrelationID string `json:"correlation_id"`
	// IssuedAt is when the API published. Compared against the time the
	// worker picks the message up, it is the queue latency — the number that
	// says whether the workers are keeping up.
	IssuedAt time.Time `json:"issued_at"`
}

// Delivery is a message taken off the queue, with the bookkeeping the consumer
// needs in order to decide what to do with it next.
type Delivery struct {
	Message TicketMessage
	// Attempt is which delivery this is, counting from one.
	Attempt int

	raw amqp.Delivery
}

// publishing turns a message into an AMQP frame.
func (m TicketMessage) publishing(attempt int) (amqp.Publishing, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return amqp.Publishing{}, fmt.Errorf("encode ticket message: %w", err)
	}

	return amqp.Publishing{
		ContentType: "application/json",
		// Persistent, so a broker restart between the sale and the
		// fulfilment does not lose the ticket. Durable queues alone do not
		// do this: the queue survives and its transient messages do not.
		DeliveryMode: amqp.Persistent,
		// MessageId is what a returned message is matched against, which is
		// how an unroutable publish is told apart from a successful one —
		// the broker acknowledges both.
		MessageId:     m.PurchaseID,
		CorrelationId: m.CorrelationID,
		Timestamp:     m.IssuedAt,
		// int64 rather than int32: widening from int is always safe, where
		// narrowing needs a bounds check to be honest about a value that
		// cannot actually exceed MaxAttempts. attemptOf reads either.
		Headers: amqp.Table{attemptHeader: int64(attempt)},
		Body:    body,
	}, nil
}

// readDelivery decodes a message and works out which attempt this is.
func readDelivery(raw amqp.Delivery) (Delivery, error) {
	var message TicketMessage
	if err := json.Unmarshal(raw.Body, &message); err != nil {
		return Delivery{}, fmt.Errorf("decode ticket message: %w", err)
	}

	return Delivery{
		Message: message,
		Attempt: attemptOf(raw),
		raw:     raw,
	}, nil
}

// attemptOf reads the attempt counter, defaulting to the first.
//
// AMQP header values arrive as whichever integer type the publisher used, and
// a message published by an older build — or by hand from the management UI —
// carries no counter at all. Treating anything unreadable as the first attempt
// is the safe direction: it costs an extra retry, where guessing high would
// send a message straight to the dead letter queue without ever trying it.
func attemptOf(raw amqp.Delivery) int {
	value, ok := raw.Headers[attemptHeader]
	if !ok {
		return 1
	}

	switch n := value.(type) {
	case int32:
		return int(n)
	case int64:
		return int(n)
	case int:
		return n
	default:
		return 1
	}
}
