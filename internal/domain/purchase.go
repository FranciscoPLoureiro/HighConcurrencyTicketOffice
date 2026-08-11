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
type Status string

const (
	// StatusConfirmed means the ticket belongs to the user.
	StatusConfirmed Status = "confirmed"
	// StatusCancelled means the purchase was reversed and the ticket
	// returned to the pool. A cancelled purchase must not keep its user out
	// of a later attempt.
	StatusCancelled Status = "cancelled"
)

// Purchase is one person's claim on one ticket.
type Purchase struct {
	ID         string
	CampaignID string
	UserID     string
	Status     Status
	CreatedAt  time.Time
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
)
