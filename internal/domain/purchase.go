// Package domain holds the vocabulary of the ticket office: what a purchase is
// and the ways one can be refused.
//
// It has no dependencies and performs no I/O, so every other package can name
// these types without dragging storage or transport along with them.
package domain

import (
	"errors"
	"time"
)

// Status is the lifecycle state of a purchase.
//
// A ticket leaves the shelf when the row is created and returns to it only
// through StatusCancelled. Every other status is a ticket somebody is holding,
// which is why the schema asks `status <> 'cancelled'` everywhere rather than
// `status = 'confirmed'`: the question that matters is whether the seat is
// taken, not whether the paperwork is finished.
type Status string

const (
	// StatusPending means Redis granted the ticket and the source of truth
	// recorded it, but fulfilment has not run yet. The holder is waiting for
	// a document, not for a decision — the seat is already theirs.
	StatusPending Status = "pending"
	// StatusConfirmed means fulfilment finished and the ticket belongs to
	// the user.
	StatusConfirmed Status = "confirmed"
	// StatusFailed means fulfilment was attempted, failed every time it was
	// retried, and the message is now in the dead letter queue.
	//
	// Deliberately not StatusCancelled. Nobody has decided to give this
	// ticket back, and reselling it while its row still says whose it is
	// would put two people in one seat. Phase 5.3 is what turns a failure
	// into a cancellation, once something has looked at it.
	StatusFailed Status = "failed"
	// StatusCancelled means the purchase was reversed and the ticket
	// returned to the pool. A cancelled purchase must not keep its user out
	// of a later attempt.
	StatusCancelled Status = "cancelled"
)

// Live reports whether this status still holds a ticket out of the pool.
//
// Phrased as "not cancelled" rather than as a list of the three that qualify,
// because that is the form the schema uses — the partial unique index and the
// reconciliation query both say `status <> 'cancelled'`. Two spellings of one
// rule drift, and the way this one would drift is that a new status nobody
// added to the list stops counting as a ticket and gets sold to someone else.
func (s Status) Live() bool { return s != StatusCancelled }

// Purchase is one person's claim on one ticket.
type Purchase struct {
	ID         string
	CampaignID string
	UserID     string
	Status     Status
	// IdempotencyKey is the UUID the client generated for the request that
	// created this row. It is what lets a retry after a timeout be
	// recognised as the same purchase rather than served as a new one.
	IdempotencyKey string
	CreatedAt      time.Time
	// UpdatedAt is when the status last changed. CreatedAt answers "when did
	// they buy?" and cannot also answer "how long has this been pending?",
	// which is the first question anyone asks when the queue backs up.
	UpdatedAt time.Time
}

// The ways a purchase can be refused.
//
// These are distinct errors rather than one because the caller has to tell them
// apart: "sold out" is final and "you already have one" is not the same
// message, and phase 2 maps each to its own response code so a client can act
// on the difference without parsing prose.
var (
	// ErrSoldOut means the campaign has no tickets left.
	ErrSoldOut = errors.New("no tickets remain")
	// ErrAlreadyPurchased means this user already holds a ticket.
	ErrAlreadyPurchased = errors.New("user already holds a ticket for this campaign")
	// ErrCampaignNotFound means the campaign does not exist.
	ErrCampaignNotFound = errors.New("campaign not found")
	// ErrPurchaseNotFound means no such purchase exists in this campaign.
	ErrPurchaseNotFound = errors.New("purchase not found")
	// ErrPurchaseNotPending means fulfilment was asked to settle a purchase
	// that is not waiting to be settled. It is a real inconsistency rather
	// than a duplicate delivery — a duplicate finds the purchase already in
	// the state it was asked to put it in, and that is not an error.
	ErrPurchaseNotPending = errors.New("purchase is not pending")
	// ErrIdempotencyKeyReplayed means this key already created a purchase.
	//
	// It is what a retry looks like once the cached response has gone: the
	// request is legitimate and the answer already exists, but the copy that
	// could have been replayed verbatim no longer does.
	ErrIdempotencyKeyReplayed = errors.New("idempotency key already used")
)
