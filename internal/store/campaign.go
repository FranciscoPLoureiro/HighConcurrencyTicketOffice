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

// RecordPending writes a ticket that Redis has already handed out.
//
// By the time this runs the decision is made: the Lua script decremented the
// stock and claimed the user, and this is the source of truth catching up. The
// two statements are one transaction so that the counter and the rows can never
// disagree, and because a purchase recorded without its decrement would survive
// a reconciliation that trusts neither on its own.
//
// The row lands as 'pending', not 'confirmed'. Fulfilment happens in the worker
// and takes seconds; the sale does not. What the row asserts from this moment
// is that the seat is taken — which is the only thing reconciliation needs to
// know, and the reason this write is on the critical path at all rather than
// left to the worker as the brief's diagram suggests.
//
// The single-row UPDATE serialises every writer on the same tuple, which is the
// contention phase 1 spent Redis to avoid — except that Redis has already
// refused everyone who was going to lose. Only winners reach this function, so
// the queue is one hundred rows deep for the whole campaign rather than five
// thousand.
func (s *Store) RecordPending(ctx context.Context, campaignID, userID, idempotencyKey string) (domain.Purchase, error) {
	purchase := domain.Purchase{
		ID:             uuid.NewString(),
		CampaignID:     campaignID,
		UserID:         userID,
		Status:         domain.StatusPending,
		IdempotencyKey: idempotencyKey,
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
		INSERT INTO purchases (id, campaign_id, user_id, status, idempotency_key)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING created_at, updated_at`,
		purchase.ID, purchase.CampaignID, purchase.UserID, purchase.Status,
		nullable(purchase.IdempotencyKey),
	).Scan(&purchase.CreatedAt, &purchase.UpdatedAt)

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
		// One of two backstops fired. Which one matters, because they mean
		// opposite things about who was wrong.
		if pgErr.ConstraintName == idempotencyKeyIndex {
			// This key already created a purchase, so this is a retry that
			// got past the idempotency check in Redis — the record expired,
			// or Redis lost it. The database remembers what Redis forgot.
			return domain.Purchase{}, domain.ErrIdempotencyKeyReplayed
		}
		// The fairness index fired, which means Redis let through a second
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

// idempotencyKeyIndex is the unique index added by migration 00003. Named here
// so that RecordPending can tell its violation apart from the fairness index's.
const idempotencyKeyIndex = "purchases_idempotency_key_idx"

// nullable turns an absent string into a SQL NULL.
//
// The empty string and NULL are different facts and the idempotency_key column
// needs the second one: ” is a value, and a unique index would let exactly one
// row hold it and reject every other purchase made without a key. NULL means
// "no key was given", and the index ignores it — which is what makes the column
// safe to add to a table that already had rows.
func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// purchaseColumns is the projection every read of a purchase uses, so that the
// scan order below can only be wrong in one place.
const purchaseColumns = `id, campaign_id, user_id, status,
	coalesce(idempotency_key::text, ''), created_at, updated_at`

func scanPurchase(row pgx.Row) (domain.Purchase, error) {
	var p domain.Purchase
	err := row.Scan(&p.ID, &p.CampaignID, &p.UserID, &p.Status,
		&p.IdempotencyKey, &p.CreatedAt, &p.UpdatedAt)
	return p, err
}

// ReadPurchase returns one purchase, scoped to its campaign.
func (s *Store) ReadPurchase(ctx context.Context, campaignID, purchaseID string) (domain.Purchase, error) {
	purchase, err := scanPurchase(s.pool.QueryRow(ctx,
		`SELECT `+purchaseColumns+` FROM purchases WHERE campaign_id = $1 AND id = $2`,
		campaignID, purchaseID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Purchase{}, domain.ErrPurchaseNotFound
	}
	if err != nil {
		return domain.Purchase{}, fmt.Errorf("read purchase: %w", err)
	}
	return purchase, nil
}

// SettlePurchase moves a pending purchase to its final state.
//
// It is idempotent by construction, and it has to be: RabbitMQ delivers at
// least once, so the worker will be handed the same message twice sooner or
// later — a redelivery after a lost ack, a retry after a timeout that the
// original attempt survived. The guard is in the WHERE clause rather than in a
// read-then-write, because a read-then-write is the phase 1 race with different
// nouns, and two workers racing on the same message would both pass it.
//
// A purchase already in the requested state is not an error: that is precisely
// what a duplicate delivery looks like from here, and the caller should go on
// to acknowledge the message rather than retry forever.
func (s *Store) SettlePurchase(ctx context.Context, purchaseID string, status domain.Status) (domain.Purchase, error) {
	purchase, err := scanPurchase(s.pool.QueryRow(ctx, `
		UPDATE purchases SET status = $2, updated_at = now()
		WHERE id = $1 AND status = $3
		RETURNING `+purchaseColumns,
		purchaseID, status, domain.StatusPending))
	if err == nil {
		return purchase, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Purchase{}, fmt.Errorf("settle purchase: %w", err)
	}

	// Nothing was pending. Either this already ran, or the row is in a state
	// no fulfilment should move it out of, and the two need different answers.
	current, err := s.readPurchaseByID(ctx, purchaseID)
	switch {
	case err != nil:
		return domain.Purchase{}, err
	case current.Status == status:
		return current, nil
	default:
		return domain.Purchase{}, fmt.Errorf("%w: purchase is %s, not %s",
			domain.ErrPurchaseNotPending, current.Status, domain.StatusPending)
	}
}

// CancelPurchase reverses a purchase and puts its ticket back on the counter.
//
// Two writes in one transaction, because they are one fact: the row stops
// holding a seat and the seat becomes available again. Split apart, a failure
// between them leaves a campaign whose counter and rows disagree, which is
// exactly what ReadCampaignState checks for and complains about.
//
// Idempotent, and it has to be for the same reason release.lua is: everything
// that reaches for this is already on a path where something went wrong once
// and may go wrong twice. The guard is in the WHERE clause, so the second call
// updates no rows and therefore increments nothing. Written as a read followed
// by a write it would be the bug the brief warns about — "if the compensation
// runs twice the stock increments twice and you now have 101 tickets" — and the
// reason it is not is that the test and the increment are the same statement.
func (s *Store) CancelPurchase(ctx context.Context, purchaseID string) (domain.Purchase, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Purchase{}, fmt.Errorf("begin cancellation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	purchase, err := scanPurchase(tx.QueryRow(ctx, `
		UPDATE purchases SET status = $2, updated_at = now()
		WHERE id = $1 AND status <> $2
		RETURNING `+purchaseColumns,
		purchaseID, domain.StatusCancelled))

	if errors.Is(err, pgx.ErrNoRows) {
		// Either it is already cancelled — which is this function having
		// already run, and not a failure — or there is no such purchase.
		current, readErr := s.readPurchaseByID(ctx, purchaseID)
		if readErr != nil {
			return domain.Purchase{}, readErr
		}
		return current, nil
	}
	if err != nil {
		return domain.Purchase{}, fmt.Errorf("cancel purchase: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE tickets SET available = available + 1 WHERE campaign_id = $1`,
		purchase.CampaignID); err != nil {
		return domain.Purchase{}, fmt.Errorf("return ticket to the counter: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.Purchase{}, fmt.Errorf("commit cancellation: %w: %w", ErrOutcomeUnknown, err)
	}

	return purchase, nil
}

func (s *Store) readPurchaseByID(ctx context.Context, purchaseID string) (domain.Purchase, error) {
	purchase, err := scanPurchase(s.pool.QueryRow(ctx,
		`SELECT `+purchaseColumns+` FROM purchases WHERE id = $1`, purchaseID))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Purchase{}, domain.ErrPurchaseNotFound
	}
	if err != nil {
		return domain.Purchase{}, fmt.Errorf("read purchase: %w", err)
	}
	return purchase, nil
}
