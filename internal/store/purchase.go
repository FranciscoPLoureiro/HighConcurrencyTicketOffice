package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// EnsureCampaign creates the campaign if it does not already exist.
//
// ON CONFLICT DO NOTHING is the whole point. Writing `available = total` on
// every start would mean a restart, a crash or a rolling deploy silently
// refilling the stock mid-campaign and selling tickets that are already gone.
// The row is created once and thereafter only ever moves downwards.
func (s *Store) EnsureCampaign(ctx context.Context, campaignID string, total int) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO tickets (campaign_id, total, available)
		VALUES ($1, $2, $2)
		ON CONFLICT (campaign_id) DO NOTHING`,
		campaignID, total)
	if err != nil {
		return fmt.Errorf("ensure campaign %q: %w", campaignID, err)
	}
	return nil
}

// PurchaseNaively sells a ticket with a check followed by an act.
//
// This implementation is deliberately wrong and phase 1 exists to prove it. The
// four statements below run on four separate pooled connections with no
// transaction and no lock, so between reading `available` and decrementing it
// any number of other requests can read the same value and reach the same
// conclusion. Every one of them then believes it has the last ticket.
//
// The race is not subtle once written down, but it is invisible in a test that
// issues one request at a time — which is exactly why it reaches production.
// Phase 2 replaces this with a single atomic operation.
func (s *Store) PurchaseNaively(ctx context.Context, campaignID, userID string) (domain.Purchase, error) {
	// 1. Is there anything left?
	var available int
	err := s.pool.QueryRow(ctx,
		`SELECT available FROM tickets WHERE campaign_id = $1`, campaignID,
	).Scan(&available)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Purchase{}, domain.ErrCampaignNotFound
	}
	if err != nil {
		return domain.Purchase{}, fmt.Errorf("read availability: %w", err)
	}
	if available <= 0 {
		return domain.Purchase{}, domain.ErrSoldOut
	}

	// 2. Has this person already bought one?
	var held int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM purchases
		WHERE campaign_id = $1 AND user_id = $2 AND status = $3`,
		campaignID, userID, domain.StatusConfirmed,
	).Scan(&held); err != nil {
		return domain.Purchase{}, fmt.Errorf("count existing purchases: %w", err)
	}
	if held > 0 {
		return domain.Purchase{}, domain.ErrAlreadyPurchased
	}

	// 3. Take the ticket.
	if _, err := s.pool.Exec(ctx,
		`UPDATE tickets SET available = available - 1 WHERE campaign_id = $1`, campaignID,
	); err != nil {
		return domain.Purchase{}, fmt.Errorf("decrement availability: %w", err)
	}

	// 4. Record who took it.
	purchase := domain.Purchase{
		ID:         uuid.NewString(),
		CampaignID: campaignID,
		UserID:     userID,
		Status:     domain.StatusConfirmed,
	}
	if err := s.pool.QueryRow(ctx, `
		INSERT INTO purchases (id, campaign_id, user_id, status)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at`,
		purchase.ID, purchase.CampaignID, purchase.UserID, purchase.Status,
	).Scan(&purchase.CreatedAt); err != nil {
		return domain.Purchase{}, fmt.Errorf("record purchase: %w", err)
	}

	return purchase, nil
}

// CountLiveTickets reports how many of the campaign's tickets are spoken for.
//
// This is the number the oversell invariant is about, and the one to compare
// against the campaign total. A ticket counts from the moment its row exists,
// not from the moment fulfilment finishes: once phase 3 made fulfilment
// asynchronous, "confirmed" started meaning "the document has been generated",
// which is a fact about a background job and not about whether the seat is
// taken. Counting confirmed rows during a campaign would report a system
// selling far fewer tickets than it has.
//
// `<> 'cancelled'` rather than a list of the states that qualify, matching
// domain.Status.Live, the partial unique index and the reconciliation query.
func (s *Store) CountLiveTickets(ctx context.Context, campaignID string) (int, error) {
	var live int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM purchases
		WHERE campaign_id = $1 AND status <> $2`,
		campaignID, domain.StatusCancelled,
	).Scan(&live); err != nil {
		return 0, fmt.Errorf("count live tickets: %w", err)
	}
	return live, nil
}

// CountByStatus reports how many purchases are in one state.
//
// Where CountLiveTickets measures the invariant, this measures progress: how
// far the workers have got, and how many gave up. Those are operational
// questions, and answering them with the same function that answers the
// correctness question is how the two get confused.
func (s *Store) CountByStatus(ctx context.Context, campaignID string, status domain.Status) (int, error) {
	var count int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM purchases
		WHERE campaign_id = $1 AND status = $2`,
		campaignID, status,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count %s purchases: %w", status, err)
	}
	return count, nil
}

// CountUsersWithMultipleTickets reports how many people hold more than one
// ticket. The fairness rule is only meaningful if it is measured.
//
// Live tickets, for the same reason as above and with a sharper edge: a person
// holding two tickets one of which is still pending is exactly what a fairness
// bug looks like while it is happening. Counting only confirmed rows would find
// nothing until the workers caught up, by which time the campaign is over.
func (s *Store) CountUsersWithMultipleTickets(ctx context.Context, campaignID string) (int, error) {
	var offenders int
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT user_id FROM purchases
			WHERE campaign_id = $1 AND status <> $2
			GROUP BY user_id HAVING count(*) > 1
		) AS duplicated`,
		campaignID, domain.StatusCancelled,
	).Scan(&offenders); err != nil {
		return 0, fmt.Errorf("count users with multiple tickets: %w", err)
	}
	return offenders, nil
}
