package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// uniqueViolation is the SQLSTATE PostgreSQL raises when a unique index rejects
// a row. Matching on the code rather than on the message keeps the check
// working across locales and server versions.
const uniqueViolation = "23505"

// ErrOutcomeUnknown means a write may or may not have been applied.
//
// Every failure before COMMIT is unambiguous: the deferred rollback runs and
// nothing was written. COMMIT is the one point where that stops being true. The
// server may have committed and lost the acknowledgement on the way back, or —
// far more likely under load — the caller's context may have expired while the
// server was busy applying it, in which case pgx abandons a connection that is
// still going to finish the job.
//
// A caller that cannot tell "it failed" from "I did not hear" must not undo
// anything, and this error exists to stop it trying. The sharper rule would be
// that a *pgconn.PgError at COMMIT proves a rollback, because the server did
// answer — but reaching that needs a deferred constraint, this schema has none,
// and a branch no test can enter is worth less than the sentence saying why it
// is absent.
var ErrOutcomeUnknown = errors.New("transaction outcome unknown")

// CampaignState is everything the startup reconciliation needs in order to
// rebuild Redis: how large the campaign is, and exactly who already holds a
// ticket.
type CampaignState struct {
	// Total is the campaign size, fixed when the campaign was created.
	Total int
	// Available is what the counter column says is left. It is redundant
	// with Total minus the live tickets, and carried here precisely so that
	// the redundancy can be checked: two representations of one fact drift,
	// and the only way to find out is to compare them.
	Available int
	// Buyers holds one entry per person with a live ticket. It is the list
	// rather than the count because Redis has to be able to answer "has this
	// person already bought?" after a restart, and a number cannot.
	Buyers []string
}

// Consistent reports whether the counter column and the purchase rows tell the
// same story.
func (s CampaignState) Consistent() bool { return s.Available == s.Total-len(s.Buyers) }

// Remaining is how much stock the source of truth says is left.
//
// Clamped at zero. A negative value is arithmetically correct and operationally
// meaningless — it would leave Redis rejecting purchases while counting
// upwards, so a campaign that was oversold once could never sell again even
// after the excess was cancelled. The caller is expected to notice and complain
// rather than to let the number through silently.
func (s CampaignState) Remaining() int {
	return max(s.Total-len(s.Buyers), 0)
}

// Oversold reports that the database holds more live tickets than the campaign
// ever had. It should be impossible from phase 2 onwards; it is exactly what a
// database left over from a phase 1 run looks like.
func (s CampaignState) Oversold() bool {
	return len(s.Buyers) > s.Total
}

// ReadCampaignState reads the campaign size and its live ticket holders.
//
// Both come from one snapshot so that a purchase committing between the two
// reads cannot produce a total and a buyer list that never coexisted. The
// isolation level is the default READ COMMITTED, under which each *statement*
// sees a fresh snapshot — which is precisely the property phase 1 was built to
// demonstrate the limits of, and precisely why this needs a transaction to
// hold one snapshot across two statements.
func (s *Store) ReadCampaignState(ctx context.Context, campaignID string) (CampaignState, error) {
	var state CampaignState

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
		IsoLevel:   pgx.RepeatableRead,
	})
	if err != nil {
		return state, fmt.Errorf("begin campaign read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	err = tx.QueryRow(ctx,
		`SELECT total, available FROM tickets WHERE campaign_id = $1`, campaignID,
	).Scan(&state.Total, &state.Available)
	if errors.Is(err, pgx.ErrNoRows) {
		return state, domain.ErrCampaignNotFound
	}
	if err != nil {
		return state, fmt.Errorf("read campaign total: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT user_id FROM purchases
		WHERE campaign_id = $1 AND status <> $2`,
		campaignID, domain.StatusCancelled)
	if err != nil {
		return state, fmt.Errorf("read campaign buyers: %w", err)
	}

	state.Buyers, err = pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return state, fmt.Errorf("collect campaign buyers: %w", err)
	}

	return state, nil
}

// RecordPurchase writes a ticket that Redis has already handed out.
//
// By the time this runs the decision is made: the Lua script decremented the
// stock and claimed the user, and this is the source of truth catching up. The
// two statements are one transaction so that the counter and the rows can never
// disagree, and because a purchase recorded without its decrement would survive
// a reconciliation that trusts neither on its own.
//
// The single-row UPDATE serialises every writer on the same tuple, which is the
// contention phase 1 spent Redis to avoid — except that Redis has already
// refused everyone who was going to lose. Only winners reach this function, so
// the queue is one hundred rows deep for the whole campaign rather than five
// thousand.
func (s *Store) RecordPurchase(ctx context.Context, campaignID, userID string) (domain.Purchase, error) {
	purchase := domain.Purchase{
		ID:         uuid.NewString(),
		CampaignID: campaignID,
		UserID:     userID,
		Status:     domain.StatusConfirmed,
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Purchase{}, fmt.Errorf("begin purchase: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx,
		`UPDATE tickets SET available = available - 1 WHERE campaign_id = $1`, campaignID)
	if err != nil {
		return domain.Purchase{}, fmt.Errorf("decrement availability: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.Purchase{}, domain.ErrCampaignNotFound
	}

	err = tx.QueryRow(ctx, `
		INSERT INTO purchases (id, campaign_id, user_id, status)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at`,
		purchase.ID, purchase.CampaignID, purchase.UserID, purchase.Status,
	).Scan(&purchase.CreatedAt)

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		// The backstop index fired, which means Redis let through a second
		// ticket for someone who already had one. Reported as the ordinary
		// refusal so the caller behaves sensibly, but it is not ordinary:
		// the two systems disagreed and the database is the one that was
		// right.
		return domain.Purchase{}, fmt.Errorf("%w: rejected by the database backstop", domain.ErrAlreadyPurchased)
	}
	if err != nil {
		return domain.Purchase{}, fmt.Errorf("record purchase: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.Purchase{}, fmt.Errorf("commit purchase: %w: %w", ErrOutcomeUnknown, err)
	}

	return purchase, nil
}
