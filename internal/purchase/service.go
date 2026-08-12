// Package purchase joins the two systems that a sale touches: Redis, which
// decides, and PostgreSQL, which remembers.
//
// It exists as its own package because that join is the interesting part of the
// design and does not belong to either side of it. The cache package knows
// nothing about purchases as records; the store package knows nothing about
// stock being enforced somewhere else.
package purchase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/cache"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/correlation"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/queue"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/store"
)

// Decider is the half of the system that decides, and Recorder is the half that
// remembers. *cache.Cache and *store.Store satisfy them, and nothing else in
// the production build does.
//
// They exist because the interesting behaviour in this package is what happens
// when one of the two halves fails, and a test that cannot make Redis or
// PostgreSQL fail on demand cannot reach any of it. The happy path is covered
// against real containers, where it belongs; the compensation policy is covered
// here, where a failure is one line of test code instead of a killed container
// and a race to hit the right instant.
type Decider interface {
	Purchase(ctx context.Context, campaignID, userID string) (cache.Outcome, int64, error)
	Release(ctx context.Context, campaignID, userID string) (bool, error)
	Remaining(ctx context.Context, campaignID string) (int64, bool, error)
	Reconcile(ctx context.Context, campaignID string, remaining int, buyers []string) error
	WithLock(ctx context.Context, key string, ttl, wait time.Duration, fn func(context.Context) error) error

	// The reservation half, added in phase 5 so that a sale interrupted
	// partway through is visible to something other than a restart.
	Confirm(ctx context.Context, campaignID, userID string) (bool, error)
	ExpiredReservations(ctx context.Context, campaignID string, olderThan time.Duration, limit int64) ([]cache.Reservation, error)
	CountReservations(ctx context.Context, campaignID string) (int64, error)
}

// Recorder is the source of truth.
type Recorder interface {
	RecordPending(ctx context.Context, campaignID, userID, idempotencyKey string) (domain.Purchase, error)
	CancelPurchase(ctx context.Context, purchaseID string) (domain.Purchase, error)
	ReadPurchase(ctx context.Context, campaignID, purchaseID string) (domain.Purchase, error)
	ReadCampaignState(ctx context.Context, campaignID string) (store.CampaignState, error)

	// What the sweeper asks before it gives a ticket back, and what it reads
	// to find sales nothing is fulfilling. MarkRepublished is how a pass
	// tells the next one that it has already dealt with a purchase.
	HasLiveTicket(ctx context.Context, campaignID, userID string) (bool, error)
	StalledPurchases(ctx context.Context, campaignID string, olderThan time.Duration, limit int) ([]domain.Purchase, error)
	MarkRepublished(ctx context.Context, purchaseID string) error
}

// Fulfiller takes a sold ticket away to be finished elsewhere.
type Fulfiller interface {
	PublishTicket(ctx context.Context, message queue.TicketMessage) error
}

// Observer counts what the service decided.
//
// It lives here rather than in the transport because this is where the words
// mean something. An HTTP layer can count 409s; only this package knows that a
// 409 was "the campaign is empty" rather than "you already have one", and the
// difference between those two numbers is the difference between a campaign
// that sold out and one that is being hammered by retries.
//
// Optional. A nil Observer is what every test that is not about metrics uses.
type Observer interface {
	TicketSold()
	TicketRejected(reason string)
	Compensated(outcome string)
}

// The reasons a sale did not happen, as metric labels.
//
// Constants rather than strings at the call sites, because a typo in a label
// does not fail anything — it silently starts a second time series that nobody
// is graphing, and the graph that exists quietly stops counting.
const (
	reasonSoldOut       = "stock_exhausted"
	reasonAlreadyHeld   = "already_purchased"
	reasonUninitialised = "campaign_not_found"
	reasonRecordFailed  = "record_failed"
	reasonPublishFailed = "publish_failed"
	reasonDeciderFailed = "decider_unavailable"
)

// Timeouts are the budgets each dependency gets inside one sale.
//
// Per call rather than one deadline over the whole thing, because the two say
// different things when they fire. A single budget only ever reports "the
// purchase was slow"; these report which system was slow, and that is the
// difference between an alert somebody can act on and one somebody investigates
// from scratch.
//
// Every one of them must be positive. A zero here would mean a context that is
// already expired by the time it reaches Redis, which fails every purchase in a
// way that looks like an outage in Redis.
type Timeouts struct {
	Redis    time.Duration
	Postgres time.Duration
	Publish  time.Duration
}

// DefaultTimeouts are used when a caller supplies none, which in practice means
// a test that is not about timing out.
var DefaultTimeouts = Timeouts{
	Redis:    2 * time.Second,
	Postgres: 5 * time.Second,
	Publish:  5 * time.Second,
}

// Service sells tickets.
type Service struct {
	cache     Decider
	store     Recorder
	fulfiller Fulfiller
	timeouts  Timeouts
	sweep     SweepPolicy
	observer  Observer
	faults    Faults
	logger    *slog.Logger
}

// WithSweepPolicy sets how long a thing has to be stuck before the sweeper acts.
func (s *Service) WithSweepPolicy(p SweepPolicy) *Service {
	if p.ReservationAge > 0 {
		s.sweep.ReservationAge = p.ReservationAge
	}
	if p.PendingAge > 0 {
		s.sweep.PendingAge = p.PendingAge
	}
	if p.Batch > 0 {
		s.sweep.Batch = p.Batch
	}
	return s
}

// WithFaults arms deliberate failures. See faults.go.
func (s *Service) WithFaults(f Faults) *Service {
	s.faults = f
	return s
}

// WithObserver attaches metrics to a Service.
//
// A separate call rather than another constructor parameter: New already takes
// five, metrics are optional everywhere they appear in this codebase, and a
// sixth argument that is nil in every test is how the other five start getting
// passed in the wrong order.
func (s *Service) WithObserver(o Observer) *Service {
	s.observer = o
	return s
}

// New builds a Service.
func New(c Decider, s Recorder, f Fulfiller, timeouts Timeouts, logger *slog.Logger) *Service {
	if timeouts.Redis <= 0 {
		timeouts.Redis = DefaultTimeouts.Redis
	}
	if timeouts.Postgres <= 0 {
		timeouts.Postgres = DefaultTimeouts.Postgres
	}
	if timeouts.Publish <= 0 {
		timeouts.Publish = DefaultTimeouts.Publish
	}

	return &Service{
		cache:     c,
		store:     s,
		fulfiller: f,
		timeouts:  timeouts,
		sweep:     DefaultSweepPolicy,
		logger:    logger,
	}
}

// compensationBudget bounds the attempt to put a ticket back after the write to
// PostgreSQL failed. It is short because the request is already lost and the
// caller is already waiting on an error.
const compensationBudget = 5 * time.Second

// Purchase sells one ticket to one user.
//
// Redis decides and PostgreSQL records. The decision is a single atomic script,
// so the two invariants — at most one hundred tickets, at most one per person —
// hold no matter how many requests arrive together.
//
// The two writes are not atomic with each other, and cannot be: they are
// separate systems. This is the gap the whole rest of the project is about, and
// it is narrowed here rather than closed. If recording the purchase is known to
// have failed, the ticket is handed back. If it cannot be known — the process
// dies between the two, or the reply to either write is simply lost — the
// ticket stays out of circulation until something notices. Phase 5 is where
// something notices.
func (s *Service) Purchase(ctx context.Context, campaignID, userID, idempotencyKey string) (domain.Purchase, error) {
	outcome, remaining, err := withBudget(ctx, s.timeouts.Redis,
		func(ctx context.Context) (cache.Outcome, int64, error) {
			return s.cache.Purchase(ctx, campaignID, userID)
		})
	if err != nil {
		// Fail closed. Redis is the only place the stock invariant is
		// enforced, so a purchase that cannot consult it is a purchase
		// nobody can prove is safe. Selling anyway would trade a failed
		// request for an oversold campaign.
		s.rejected(reasonDeciderFailed)
		return domain.Purchase{}, fmt.Errorf("decide purchase: %w", err)
	}

	switch outcome {
	case cache.AlreadyHeld:
		s.rejected(reasonAlreadyHeld)
		return domain.Purchase{}, domain.ErrAlreadyPurchased
	case cache.SoldOut:
		s.rejected(reasonSoldOut)
		return domain.Purchase{}, domain.ErrSoldOut
	case cache.Uninitialised:
		s.rejected(reasonUninitialised)
		// Not a refusal — a system that has not been told what it is
		// selling. Startup reconciliation is supposed to make this
		// unreachable, so it is worth a loud line rather than a quiet 404.
		s.logger.Error("purchase attempted against a campaign with no stock in redis",
			slog.String("campaign_id", campaignID))
		return domain.Purchase{}, domain.ErrCampaignNotFound
	case cache.Sold:
	}

	// The ticket has left the shelf and nothing durable knows it yet. This is
	// the window phase 5 exists for, and the fault injector opens it on
	// purpose so that a test can prove the sweeper closes it.
	if err := s.faults.after(FaultAfterDecrement); err != nil {
		return domain.Purchase{}, err
	}

	purchase, err := withBudget2(ctx, s.timeouts.Postgres,
		func(ctx context.Context) (domain.Purchase, error) {
			return s.store.RecordPending(ctx, campaignID, userID, idempotencyKey)
		})
	if err != nil {
		// Hand the ticket back only when the write is known not to have
		// happened. A failure at COMMIT does not say that, and treating it as
		// though it did is how a hundred tickets becomes a hundred and one:
		// if the transaction committed after all, the compensation returns
		// stock that PostgreSQL has already given away and un-marks a buyer
		// who really does hold a ticket.
		//
		// Doing nothing costs at most one ticket that nobody can buy until
		// the next reconciliation. That is the direction this system errs in
		// everywhere else, and the only one of the two that is recoverable.
		s.rejected(reasonRecordFailed)

		if errors.Is(err, store.ErrOutcomeUnknown) {
			s.logger.Error("purchase may or may not have been recorded, leaving the ticket out of circulation",
				slog.String("campaign_id", campaignID),
				slog.String("user_id", userID),
				slog.Any("error", err))
			return domain.Purchase{}, fmt.Errorf("record purchase: %w", err)
		}

		s.compensate(ctx, campaignID, userID, err)
		return domain.Purchase{}, fmt.Errorf("record purchase: %w", err)
	}

	// The row exists and the message is not sent. The second crash window,
	// and the one the sweeper republishes out of.
	if err := s.faults.after(FaultAfterRecord); err != nil {
		return domain.Purchase{}, err
	}

	if err := s.handOver(ctx, purchase); err != nil {
		s.rejected(reasonPublishFailed)
		return domain.Purchase{}, err
	}

	// The sale is accounted for everywhere that matters, so the reservation
	// has nothing left to protect. Failing to close it is not worth failing
	// the request over: the sweeper will find it open, ask PostgreSQL, see a
	// real purchase and close it then. One wasted read against a purchase the
	// caller has already been promised.
	if _, err := s.cache.Confirm(ctx, campaignID, userID); err != nil {
		s.logger.Warn("sold a ticket but could not close its reservation",
			slog.String("campaign_id", campaignID),
			slog.String("user_id", userID),
			slog.Any("error", err))
	}

	if s.observer != nil {
		s.observer.TicketSold()
	}

	s.logger.Debug("ticket sold",
		slog.String("campaign_id", campaignID),
		slog.String("user_id", userID),
		slog.String("purchase_id", purchase.ID),
		slog.Int64("remaining", remaining))

	return purchase, nil
}

// ReadPurchase reports what the source of truth currently says about a
// purchase.
//
// Scoped to the campaign here and to its owner at the transport layer: a
// correlation id handed out in a 202 is a receipt, not a licence to read other
// people's tickets.
func (s *Service) ReadPurchase(ctx context.Context, campaignID, purchaseID string) (domain.Purchase, error) {
	return withBudget2(ctx, s.timeouts.Postgres, func(ctx context.Context) (domain.Purchase, error) {
		return s.store.ReadPurchase(ctx, campaignID, purchaseID)
	})
}

// handOver publishes the ticket for fulfilment and deals with a publish that
// did not happen.
//
// This is the second of the three writes a sale makes, and the one the brief
// calls the most interesting problem in the system. It is still not atomic with
// the other two — a broker is a third system — but it is now the only gap left,
// and what happens on failure depends entirely on whether the failure is
// something this process can be sure of.
func (s *Service) handOver(ctx context.Context, purchase domain.Purchase) error {
	message := queue.TicketMessage{
		PurchaseID:     purchase.ID,
		CampaignID:     purchase.CampaignID,
		UserID:         purchase.UserID,
		IdempotencyKey: purchase.IdempotencyKey,
		CorrelationID:  correlation.From(ctx),
		IssuedAt:       time.Now().UTC(),
	}

	publishCtx, cancel := context.WithTimeout(ctx, s.timeouts.Publish)
	defer cancel()

	err := s.fulfiller.PublishTicket(publishCtx, message)
	if err == nil {
		return nil
	}

	// The broker considered the message and said no, or took it and had
	// nowhere to put it. Both are definitive: nothing holds this ticket, and
	// undoing the sale returns it to somebody who can actually have it.
	if errors.Is(err, queue.ErrPublishRefused) || errors.Is(err, queue.ErrUnroutable) {
		s.reverse(ctx, purchase, err)
		return fmt.Errorf("publish ticket: %w", err)
	}

	// Anything else is an unconfirmed publish, and undoing on one of those is
	// the same mistake as compensating an unknown commit: the broker may have
	// the message and have been slow to say so, and a worker is about to
	// fulfil a purchase this process just cancelled.
	//
	// The row stays pending. It is durable, it counts as a live ticket, and
	// it is what the phase 5 sweeper will eventually find and resolve. Until
	// then it is a ticket nobody can buy, which is the survivable half of the
	// problem.
	s.logger.Error("ticket may or may not have been queued, leaving the purchase pending",
		slog.String("campaign_id", purchase.CampaignID),
		slog.String("user_id", purchase.UserID),
		slog.String("purchase_id", purchase.ID),
		slog.Any("error", err))

	return fmt.Errorf("publish ticket: %w", err)
}

// reverse undoes a sale that will never be fulfilled.
//
// The order is not arbitrary. PostgreSQL first, then Redis: if the second half
// fails, Redis is left one ticket short of the truth, the campaign undersells
// by one, and the next reconciliation puts it right. The other order fails the
// other way — the ticket is back on the shelf while a live row still says whose
// it is — and that is an oversell plus a person who cannot buy again because the
// fairness index remembers them.
func (s *Service) reverse(ctx context.Context, purchase domain.Purchase, cause error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), compensationBudget)
	defer cancel()

	if _, err := s.store.CancelPurchase(ctx, purchase.ID); err != nil { //nolint:contextcheck // the detached context above is deliberate
		// The row still says the seat is taken, so Redis must go on saying
		// the same thing. Releasing now would be the two systems disagreeing
		// in the direction that oversells.
		s.logger.Error("could not cancel a purchase that was never queued",
			slog.String("purchase_id", purchase.ID),
			slog.Any("cause", cause),
			slog.Any("error", err))
		return
	}

	s.compensate(ctx, purchase.CampaignID, purchase.UserID, cause)
}

// rejected counts a sale that did not happen, when anything is counting.
func (s *Service) rejected(reason string) {
	if s.observer != nil {
		s.observer.TicketRejected(reason)
	}
}

// compensate returns a ticket that Redis granted and PostgreSQL refused.
//
// Without this the ticket is decremented from a stock it will never leave: it
// belongs to nobody, cannot be sold, and the campaign quietly ends one ticket
// short for every failed write.
func (s *Service) compensate(ctx context.Context, campaignID, userID string, cause error) {
	// A detached context. The usual reason RecordPurchase failed is that the
	// caller's context was cancelled or timed out, and reusing it here would
	// mean the compensation fails for the same reason, every time — leaking
	// a ticket in exactly the case the compensation was written for.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), compensationBudget)
	defer cancel()

	released, err := s.cache.Release(ctx, campaignID, userID)
	if err != nil {
		// Both writes have now failed. The ticket stays out of circulation
		// until reconciliation next runs, which is the safe direction — the
		// campaign undersells by one rather than overselling by one.
		s.logger.Error("could not return a ticket after the purchase failed to record",
			slog.String("campaign_id", campaignID),
			slog.String("user_id", userID),
			slog.Any("cause", cause),
			slog.Any("error", err))
		// The alerting one. A failed compensation means a ticket is out of
		// circulation and nothing but the next reconciliation will notice.
		if s.observer != nil {
			s.observer.Compensated("failed")
		}
		return
	}

	if s.observer != nil {
		s.observer.Compensated("released")
	}

	s.logger.Warn("returned a ticket after the purchase failed to record",
		slog.String("campaign_id", campaignID),
		slog.String("user_id", userID),
		slog.Bool("released", released),
		slog.Any("cause", cause))
}

// withBudget and withBudget2 run one dependency call under its own deadline.
//
// Two of them because Go generics cannot abstract over arity, and the
// alternative — a context.WithTimeout and a defer at every call site — is four
// lines of ceremony around each one that buries what the call actually does.
func withBudget[A, B any](ctx context.Context, budget time.Duration,
	call func(context.Context) (A, B, error),
) (A, B, error) {
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	return call(ctx)
}

func withBudget2[A any](ctx context.Context, budget time.Duration,
	call func(context.Context) (A, error),
) (A, error) {
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	return call(ctx)
}
