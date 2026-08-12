package purchase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/cache"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/queue"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/store"
)

// The tests in this file are about what the service does after something has
// already gone wrong. That is the part a real Redis and a real PostgreSQL
// cannot help with: they succeed, and the interesting branches never run.
//
// Everything about atomicity is tested against containers instead, in the
// files behind the integration tag. These two suites are not alternatives.

// testKey is a fixed idempotency key. These tests never send two requests, so
// the value only has to be a plausible UUID and the same one throughout.
const testKey = "3f1a0c9e-0000-4000-8000-000000000001"

// stubDecider stands in for Redis.
type stubDecider struct {
	outcome   cache.Outcome
	remaining int64
	purchErr  error

	// releases counts calls to Release, which is the whole assertion in most
	// of these tests: compensation either ran or it did not.
	releases   int
	releaseErr error

	// confirms counts reservations closed, and expired is what the sweeper
	// will be handed.
	confirms   int
	confirmErr error
	expired    []cache.Reservation
}

func (d *stubDecider) Purchase(context.Context, string, string) (cache.Outcome, int64, error) {
	return d.outcome, d.remaining, d.purchErr
}

func (d *stubDecider) Release(context.Context, string, string) (bool, error) {
	d.releases++
	return true, d.releaseErr
}

func (d *stubDecider) Remaining(context.Context, string) (int64, bool, error) {
	return d.remaining, true, nil
}

func (d *stubDecider) Reconcile(context.Context, string, int, []string) error { return nil }

func (d *stubDecider) WithLock(ctx context.Context, _ string, _, _ time.Duration, fn func(context.Context) error) error {
	return fn(ctx)
}

func (d *stubDecider) Confirm(context.Context, string, string) (bool, error) {
	d.confirms++
	return true, d.confirmErr
}

func (d *stubDecider) ExpiredReservations(context.Context, string, time.Duration, int64) ([]cache.Reservation, error) {
	return d.expired, nil
}

func (d *stubDecider) CountReservations(context.Context, string) (int64, error) {
	return int64(len(d.expired)), nil
}

// stubRecorder stands in for PostgreSQL.
type stubRecorder struct {
	err error

	// cancellations counts the rows reversed, which is how a test tells a
	// sale that was undone from one merely reported as failed.
	cancellations int
	cancelErr     error

	// What the sweeper is told about the source of truth. liveLookup, when
	// set, overrides the fixed answer so a test can vary it per call.
	liveTicket bool
	liveErr    error
	liveLookup func() (bool, error)
	stalled    []domain.Purchase

	// republished stands in for the timestamp the real store bumps. A
	// purchase that has been marked has not sat unchanged for PendingAge any
	// more, so the next pass does not see it — which is the whole point of
	// the mark and cannot be observed at all if the stub keeps handing back
	// the same list.
	republished map[string]bool
	markErr     error
}

func (r *stubRecorder) RecordPending(_ context.Context, campaignID, userID, idempotencyKey string) (domain.Purchase, error) {
	if r.err != nil {
		return domain.Purchase{}, r.err
	}
	return domain.Purchase{
		ID: "purchase-1", CampaignID: campaignID, UserID: userID,
		Status: domain.StatusPending, IdempotencyKey: idempotencyKey,
	}, nil
}

func (r *stubRecorder) ReadCampaignState(context.Context, string) (store.CampaignState, error) {
	return store.CampaignState{}, nil
}

func (r *stubRecorder) CancelPurchase(_ context.Context, purchaseID string) (domain.Purchase, error) {
	r.cancellations++
	if r.cancelErr != nil {
		return domain.Purchase{}, r.cancelErr
	}
	return domain.Purchase{ID: purchaseID, Status: domain.StatusCancelled}, nil
}

func (r *stubRecorder) ReadPurchase(_ context.Context, campaignID, purchaseID string) (domain.Purchase, error) {
	return domain.Purchase{ID: purchaseID, CampaignID: campaignID}, nil
}

func (r *stubRecorder) HasLiveTicket(context.Context, string, string) (bool, error) {
	if r.liveLookup != nil {
		return r.liveLookup()
	}
	return r.liveTicket, r.liveErr
}

func (r *stubRecorder) StalledPurchases(context.Context, string, time.Duration, int) ([]domain.Purchase, error) {
	stalled := make([]domain.Purchase, 0, len(r.stalled))
	for _, purchase := range r.stalled {
		if !r.republished[purchase.ID] {
			stalled = append(stalled, purchase)
		}
	}
	return stalled, nil
}

func (r *stubRecorder) MarkRepublished(_ context.Context, purchaseID string) error {
	if r.markErr != nil {
		return r.markErr
	}
	if r.republished == nil {
		r.republished = make(map[string]bool)
	}
	r.republished[purchaseID] = true
	return nil
}

// stubFulfiller stands in for RabbitMQ.
type stubFulfiller struct {
	sent []queue.TicketMessage
	err  error
}

func (f *stubFulfiller) PublishTicket(_ context.Context, message queue.TicketMessage) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, message)
	return nil
}

func newTestService(d *stubDecider, r *stubRecorder) *Service {
	return newTestServiceWith(d, r, &stubFulfiller{})
}

func newTestServiceWith(d *stubDecider, r *stubRecorder, f *stubFulfiller) *Service {
	return New(d, r, f, Timeouts{}, slog.New(slog.DiscardHandler))
}

// A write that is known to have failed must give the ticket back.
//
// Without this the ticket is decremented from a stock it will never leave: it
// belongs to nobody, cannot be sold, and the campaign ends one ticket short for
// every failed write.
func TestAKnownFailedWriteReturnsTheTicket(t *testing.T) {
	decider := &stubDecider{outcome: cache.Sold, remaining: 41}
	recorder := &stubRecorder{err: errors.New("connection refused")}

	_, err := newTestService(decider, recorder).Purchase(context.Background(), "queima", "student-1", testKey)
	if err == nil {
		t.Fatal("Purchase() = nil, want an error")
	}

	if decider.releases != 1 {
		t.Errorf("Release called %d times, want 1", decider.releases)
	}
}

// A write whose fate is unknown must not.
//
// This is the bug this test was written for. RecordPending reports
// ErrOutcomeUnknown when it failed at COMMIT, which is the one point where the
// transaction may have been applied anyway — the server committed and the
// acknowledgement was lost, or the caller's context expired while the server
// was still working. Compensating there returns stock that PostgreSQL has
// already given away and un-marks a buyer who really does hold a ticket, and
// the campaign can then sell a hundred and first.
//
// Leaving it alone costs one ticket until the next reconciliation. That is the
// recoverable direction, and it is the one the rest of the system takes.
func TestAWriteWithAnUnknownOutcomeKeepsTheTicketOutOfCirculation(t *testing.T) {
	decider := &stubDecider{outcome: cache.Sold, remaining: 41}
	recorder := &stubRecorder{
		err: fmt.Errorf("commit purchase: %w: %w", store.ErrOutcomeUnknown, context.DeadlineExceeded),
	}

	_, err := newTestService(decider, recorder).Purchase(context.Background(), "queima", "student-1", testKey)
	if !errors.Is(err, store.ErrOutcomeUnknown) {
		t.Fatalf("Purchase() = %v, want it to wrap %v", err, store.ErrOutcomeUnknown)
	}

	if decider.releases != 0 {
		t.Errorf("Release called %d times after an unknown outcome, want 0 — "+
			"compensating here is what oversells the campaign", decider.releases)
	}
}

// Compensation runs on a cancelled request, because that is when it is needed
// most.
//
// The usual reason the write failed is that the caller gave up, and a
// compensation inheriting that context fails for the same reason every time —
// leaking a ticket in exactly the case it was written for.
func TestCompensationSurvivesTheCancellationThatCausedIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	decider := &stubDecider{outcome: cache.Sold, remaining: 41}
	recorder := &stubRecorder{err: context.Canceled}

	if _, err := newTestService(decider, recorder).Purchase(ctx, "queima", "student-1", testKey); err == nil {
		t.Fatal("Purchase() = nil, want an error")
	}

	if decider.releases != 1 {
		t.Errorf("Release called %d times on a cancelled request, want 1", decider.releases)
	}
}

// The refusals are not failures, and nothing is compensated for them: the
// script did not take a ticket, so there is nothing to give back.
func TestRefusalsCompensateNothing(t *testing.T) {
	tests := []struct {
		name    string
		outcome cache.Outcome
		want    error
	}{
		{"already holds one", cache.AlreadyHeld, domain.ErrAlreadyPurchased},
		{"campaign is empty", cache.SoldOut, domain.ErrSoldOut},
		{"redis was never reconciled", cache.Uninitialised, domain.ErrCampaignNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decider := &stubDecider{outcome: tt.outcome}

			_, err := newTestService(decider, &stubRecorder{}).Purchase(
				context.Background(), "queima", "student-1", testKey)
			if !errors.Is(err, tt.want) {
				t.Errorf("Purchase() = %v, want %v", err, tt.want)
			}

			if decider.releases != 0 {
				t.Errorf("Release called %d times for a refusal, want 0", decider.releases)
			}
		})
	}
}

// Redis failing is not a reason to sell a ticket nobody can prove is there.
func TestADeciderThatCannotAnswerFailsClosed(t *testing.T) {
	decider := &stubDecider{purchErr: errors.New("dial tcp: connection refused")}
	recorder := &stubRecorder{}

	if _, err := newTestService(decider, recorder).Purchase(
		context.Background(), "queima", "student-1", testKey); err == nil {
		t.Fatal("Purchase() = nil, want an error — selling without Redis oversells")
	}
}
