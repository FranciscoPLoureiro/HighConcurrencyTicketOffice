package purchase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/cache"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
)

// The sweeper is the one component here that can *cause* an oversell.
//
// Everything else in this system errs towards keeping a ticket off the shelf.
// This is the only thing whose job is to put one back, so the question these
// tests ask is not "does it recover tickets" — the integration suite proves
// that against real containers — but "does it ever recover one it should not".

func expiredFor(users ...string) []cache.Reservation {
	reservations := make([]cache.Reservation, 0, len(users))
	for _, user := range users {
		reservations = append(reservations, cache.Reservation{
			UserID: user,
			Taken:  time.Now().Add(-5 * time.Minute),
		})
	}
	return reservations
}

// A reservation whose sale completed must not have its ticket taken back.
//
// This is the failure that would sell one seat twice, and the reason the
// database is asked before anything is released. A reservation left open only
// says a ticket left the shelf; whether the sale finished is a fact the source
// of truth holds, and it is the only opinion that counts.
func TestASweptReservationBackedByARealPurchaseIsNotReleased(t *testing.T) {
	decider := &stubDecider{expired: expiredFor("student-1")}
	recorder := &stubRecorder{liveTicket: true}

	result, err := newTestService(decider, recorder).Sweep(context.Background(), "queima")
	if err != nil {
		t.Fatalf("Sweep() = %v", err)
	}

	if decider.releases != 0 {
		t.Errorf("released %d tickets that PostgreSQL says are sold — this is an oversell",
			decider.releases)
	}
	if result.Released != 0 {
		t.Errorf("reported %d released, want 0", result.Released)
	}
	// It is still tidied up, or the sweeper looks at it again forever.
	if decider.confirms != 1 {
		t.Errorf("closed %d stale reservations, want 1", decider.confirms)
	}
	if result.Closed != 1 {
		t.Errorf("reported %d closed, want 1", result.Closed)
	}
}

// A reservation the source of truth has never heard of is the ticket this
// whole mechanism exists to recover.
func TestASweptReservationWithNoPurchaseGivesTheTicketBack(t *testing.T) {
	decider := &stubDecider{expired: expiredFor("student-1", "student-2")}
	recorder := &stubRecorder{liveTicket: false}

	result, err := newTestService(decider, recorder).Sweep(context.Background(), "queima")
	if err != nil {
		t.Fatalf("Sweep() = %v", err)
	}

	if decider.releases != 2 {
		t.Errorf("released %d abandoned tickets, want 2", decider.releases)
	}
	if result.Released != 2 {
		t.Errorf("reported %d released, want 2", result.Released)
	}
}

// A database that cannot answer must not be treated as a database saying no.
//
// "PostgreSQL has never heard of this sale" and "PostgreSQL did not reply" look
// identical if the error is ignored, and acting on the second releases tickets
// people are holding. The reservation stays where it is and the next pass asks
// again.
func TestASweepDoesNotReleaseWhenTheDatabaseCannotAnswer(t *testing.T) {
	decider := &stubDecider{expired: expiredFor("student-1")}
	recorder := &stubRecorder{liveErr: errors.New("connection refused")}

	result, err := newTestService(decider, recorder).Sweep(context.Background(), "queima")
	if err != nil {
		t.Fatalf("Sweep() = %v, want the pass to survive one bad entry", err)
	}

	if decider.releases != 0 {
		t.Errorf("released %d tickets on a failed lookup — an unanswered query is not a no",
			decider.releases)
	}
	if result.Released != 0 || result.Closed != 0 {
		t.Errorf("settled %d+%d reservations despite the error", result.Released, result.Closed)
	}
}

// One unreadable entry must not stop the pass, or a single stuck reservation
// holds up the recovery of every other ticket behind it.
func TestOneBadEntryDoesNotAbandonTheRestOfThePass(t *testing.T) {
	decider := &stubDecider{expired: expiredFor("student-1", "student-2", "student-3")}
	recorder := &failOnceRecorder{}

	result, err := newTestService(decider, recorder.asStub()).Sweep(context.Background(), "queima")
	if err != nil {
		t.Fatalf("Sweep() = %v", err)
	}

	if result.Examined != 3 {
		t.Errorf("examined %d reservations, want 3", result.Examined)
	}
}

// A purchase nothing is fulfilling is sent back for fulfilment.
//
// The second crash window: the row committed and the publish did not, so the
// seat is taken and no worker will ever produce a ticket for it. Republishing
// is safe because settling is idempotent — a duplicate finds nothing to do.
func TestAStalledPurchaseIsRepublished(t *testing.T) {
	decider := &stubDecider{}
	recorder := &stubRecorder{stalled: []domain.Purchase{
		{ID: "purchase-1", CampaignID: "queima", UserID: "student-1", Status: domain.StatusPending},
	}}
	fulfiller := &stubFulfiller{}

	result, err := newTestServiceWith(decider, recorder, fulfiller).Sweep(context.Background(), "queima")
	if err != nil {
		t.Fatalf("Sweep() = %v", err)
	}

	if result.Republished != 1 {
		t.Errorf("republished %d stalled purchases, want 1", result.Republished)
	}
	if len(fulfiller.sent) != 1 {
		t.Fatalf("published %d messages, want 1", len(fulfiller.sent))
	}
	if fulfiller.sent[0].PurchaseID != "purchase-1" {
		t.Errorf("republished %q, want purchase-1", fulfiller.sent[0].PurchaseID)
	}
	// Marked as the sweeper's, so a worker log line joins to something that
	// explains where the message came from.
	if fulfiller.sent[0].CorrelationID != "sweeper:purchase-1" {
		t.Errorf("correlation id = %q, want it to name the sweeper",
			fulfiller.sent[0].CorrelationID)
	}
}

// One republish per PendingAge, not one per pass.
//
// Sending the message does not change the row, so nothing about the purchase
// tells the next pass it has already been dealt with. Left that way the
// sweeper re-sends every stalled purchase every SweepInterval — four times a
// minute with the defaults — and a worker returning from an outage works
// through several times its real backlog, spending a full pretend fulfilment
// on each duplicate before finding there is nothing to do.
func TestAStalledPurchaseIsNotRepublishedOnEveryPass(t *testing.T) {
	decider := &stubDecider{}
	recorder := &stubRecorder{stalled: []domain.Purchase{
		{ID: "purchase-1", CampaignID: "queima", UserID: "student-1", Status: domain.StatusPending},
	}}
	fulfiller := &stubFulfiller{}
	service := newTestServiceWith(decider, recorder, fulfiller)

	for pass := 1; pass <= 3; pass++ {
		if _, err := service.Sweep(context.Background(), "queima"); err != nil {
			t.Fatalf("Sweep() pass %d = %v", pass, err)
		}
	}

	if len(fulfiller.sent) != 1 {
		t.Errorf("sent %d messages over three passes for one stalled purchase, want 1",
			len(fulfiller.sent))
	}
}

// A republish the broker refused must not be recorded as one, or the purchase
// is held back for another PendingAge over an attempt that never happened.
func TestARefusedRepublishIsNotRecordedAsOne(t *testing.T) {
	decider := &stubDecider{}
	recorder := &stubRecorder{stalled: []domain.Purchase{
		{ID: "purchase-1", CampaignID: "queima", UserID: "student-1", Status: domain.StatusPending},
	}}
	fulfiller := &stubFulfiller{err: errors.New("broker unreachable")}
	service := newTestServiceWith(decider, recorder, fulfiller)

	if _, err := service.Sweep(context.Background(), "queima"); err != nil {
		t.Fatalf("Sweep() = %v", err)
	}
	if recorder.republished["purchase-1"] {
		t.Fatal("a publish that failed was recorded as a republish")
	}

	fulfiller.err = nil
	result, err := service.Sweep(context.Background(), "queima")
	if err != nil {
		t.Fatalf("second Sweep() = %v", err)
	}
	if result.Republished != 1 {
		t.Errorf("republished %d once the broker recovered, want 1", result.Republished)
	}
}

// A broker that will not take the message leaves the purchase alone. It is
// still pending, still stalled, and still there for the next pass.
func TestARepublishThatFailsLeavesThePurchaseForNextTime(t *testing.T) {
	decider := &stubDecider{}
	recorder := &stubRecorder{stalled: []domain.Purchase{
		{ID: "purchase-1", CampaignID: "queima", UserID: "student-1"},
	}}
	fulfiller := &stubFulfiller{err: errors.New("broker unreachable")}

	result, err := newTestServiceWith(decider, recorder, fulfiller).Sweep(context.Background(), "queima")
	if err != nil {
		t.Fatalf("Sweep() = %v, want the pass to survive a failed publish", err)
	}
	if result.Republished != 0 {
		t.Errorf("reported %d republished after a failed publish, want 0", result.Republished)
	}
}

// failOnceRecorder answers the first lookup with an error and the rest
// normally, which is what an intermittent database looks like.
type failOnceRecorder struct{ calls int }

func (f *failOnceRecorder) asStub() *stubRecorder {
	return &stubRecorder{liveLookup: func() (bool, error) {
		f.calls++
		if f.calls == 1 {
			return false, errors.New("connection reset")
		}
		return true, nil
	}}
}
