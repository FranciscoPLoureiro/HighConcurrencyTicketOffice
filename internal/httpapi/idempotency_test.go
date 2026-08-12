package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/health"
)

const testIdempotencyKey = "6f9619ff-8b86-d011-b42d-00cf4fc964ff"

// memoryIdempotencyStore is the Redis script's behaviour in a map.
//
// It is a fake rather than a stub because these tests are about a sequence —
// claim, work, store, claim again — and a stub returning a fixed answer cannot
// express one. What it must not be is a fake of the *atomicity*: that is tested
// against a real Redis behind the integration tag, because a mutex in this
// process proves nothing about a Lua script in another one.
type memoryIdempotencyStore struct {
	mu      sync.Mutex
	records map[string]string

	claimErr error
	storeErr error
	released []string
}

func newMemoryStore() *memoryIdempotencyStore {
	return &memoryIdempotencyStore{records: make(map[string]string)}
}

func (m *memoryIdempotencyStore) ClaimIdempotency(_ context.Context, key string, _ time.Duration) (domain.Claim, string, error) {
	if m.claimErr != nil {
		return 0, "", m.claimErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.records[key]
	if !ok {
		m.records[key] = "P"
		return domain.ClaimAccepted, "", nil
	}
	if existing[:1] == "D" {
		return domain.ClaimReplayed, existing[1:], nil
	}
	return domain.ClaimInFlight, "", nil
}

func (m *memoryIdempotencyStore) StoreResponse(_ context.Context, key, response string, _ time.Duration) error {
	if m.storeErr != nil {
		return m.storeErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[key] = "D" + response
	return nil
}

func (m *memoryIdempotencyStore) ReleaseClaim(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.released = append(m.released, key)
	delete(m.records, key)
	return nil
}

// countingHandler records how many times the work actually ran, which is the
// only thing any of these tests is really asking about.
type countingHandler struct {
	calls  int
	status int
	body   string
}

func (h *countingHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	h.calls++
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(h.status)
	_, _ = w.Write([]byte(h.body))
}

func idempotentRoutes(t *testing.T, store IdempotencyStore, handler http.Handler) http.Handler {
	t.Helper()

	s := New(Config{
		Health:     health.New(),
		CampaignID: testCampaign,
		Logger:     slog.New(slog.DiscardHandler),
		Idempotency: func() IdempotencyStore {
			return store
		}(),
		IdempotencyPolicy: IdempotencyPolicy{Lease: time.Minute, Retention: time.Hour},
	})

	return withIdentity(s.withIdempotency(handler))
}

func postWithKey(handler http.Handler, userID, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tickets/purchase", nil)
	req.Header.Set(userIDHeader, userID)
	if key != "" {
		req.Header.Set(idempotencyKeyHeader, key)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// The test the whole mechanism exists for.
//
// A client whose request timed out sends it again with the same key. The work
// must not run twice, and the second answer must be the first one — not a
// refusal explaining that they already hold the ticket they cannot see.
func TestARetryWithTheSameKeyGetsTheOriginalAnswerAndDoesNoWork(t *testing.T) {
	handler := &countingHandler{status: http.StatusAccepted, body: `{"purchase_id":"abc"}`}
	routes := idempotentRoutes(t, newMemoryStore(), handler)

	first := postWithKey(routes, "student-1", testIdempotencyKey)
	second := postWithKey(routes, "student-1", testIdempotencyKey)

	if handler.calls != 1 {
		t.Errorf("the handler ran %d times, want 1 — the retry consumed a second ticket", handler.calls)
	}
	if first.Code != http.StatusAccepted || second.Code != first.Code {
		t.Errorf("statuses were %d then %d, want %d twice", first.Code, second.Code, http.StatusAccepted)
	}
	if first.Body.String() != second.Body.String() {
		t.Errorf("replay returned %q, want the original %q", second.Body.String(), first.Body.String())
	}
	if second.Header().Get(replayHeader) != "true" {
		t.Errorf("%s = %q on the replay, want \"true\"", replayHeader, second.Header().Get(replayHeader))
	}
	if first.Header().Get(replayHeader) != "" {
		t.Errorf("%s was set on the first answer, which was not a replay", replayHeader)
	}
}

// A refusal is an answer, and a retry should hear the same one.
//
// The alternative — releasing the key on a 409 — lets a client retry its way
// into a different outcome, which is the opposite of what an idempotency key
// promises.
func TestARefusalIsReplayedRatherThanReDecided(t *testing.T) {
	handler := &countingHandler{
		status: http.StatusConflict,
		body:   `{"error":{"code":"stock_exhausted","message":"there are no tickets left"}}`,
	}
	routes := idempotentRoutes(t, newMemoryStore(), handler)

	postWithKey(routes, "student-1", testIdempotencyKey)
	second := postWithKey(routes, "student-1", testIdempotencyKey)

	if handler.calls != 1 {
		t.Errorf("the handler ran %d times, want 1", handler.calls)
	}
	if second.Code != http.StatusConflict {
		t.Errorf("replayed status = %d, want %d", second.Code, http.StatusConflict)
	}
	if got := decodeErrorCode(t, second); got != "stock_exhausted" {
		t.Errorf("replayed code = %q, want stock_exhausted", got)
	}
}

// A 5xx is not an answer, so the key has to come back.
//
// The client's only recourse is to send the request again, and the only key
// that can be recognised is the one they already used. Holding it would lock
// them out of the retry for the length of the lease.
func TestAServerErrorGivesTheKeyBack(t *testing.T) {
	store := newMemoryStore()
	handler := &countingHandler{status: http.StatusInternalServerError, body: `{"error":{"code":"internal_error"}}`}
	routes := idempotentRoutes(t, store, handler)

	postWithKey(routes, "student-1", testIdempotencyKey)
	if len(store.released) != 1 {
		t.Fatalf("the key was released %d times after a 500, want 1", len(store.released))
	}

	postWithKey(routes, "student-1", testIdempotencyKey)
	if handler.calls != 2 {
		t.Errorf("the handler ran %d times across two attempts, want 2 — the retry was locked out", handler.calls)
	}
}

// Two attempts with one key, the first still running.
//
// There is no answer to give, and doing the work again to invent one is the
// duplicate the key exists to prevent. 409 says "ask me again shortly", which
// is safe precisely because the key makes asking again free.
func TestASecondAttemptWhileTheFirstIsStillRunningIsRefused(t *testing.T) {
	store := newMemoryStore()
	handler := &countingHandler{status: http.StatusAccepted, body: `{}`}
	routes := idempotentRoutes(t, store, handler)

	// The claim exists with no answer behind it, which is exactly what an
	// in-flight first attempt leaves in Redis.
	store.records[cacheKey("student-1")] = "P"

	rec := postWithKey(routes, "student-1", testIdempotencyKey)
	if rec.Code != http.StatusConflict {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusConflict)
	}
	if got := decodeErrorCode(t, rec); got != "idempotency_key_in_use" {
		t.Errorf("code = %q, want idempotency_key_in_use", got)
	}
	if handler.calls != 0 {
		t.Errorf("the handler ran %d times behind an in-flight claim, want 0", handler.calls)
	}
}

// One person's key must not unlock another person's answer.
//
// The key is a value the client picks, so scoping records to the campaign alone
// would let anyone who learned somebody else's key read the purchase it made.
func TestTwoPeopleUsingTheSameKeyDoNotShareAnAnswer(t *testing.T) {
	handler := &countingHandler{status: http.StatusAccepted, body: `{}`}
	routes := idempotentRoutes(t, newMemoryStore(), handler)

	postWithKey(routes, "student-1", testIdempotencyKey)
	second := postWithKey(routes, "student-2", testIdempotencyKey)

	if handler.calls != 2 {
		t.Errorf("the handler ran %d times for two different people, want 2", handler.calls)
	}
	if second.Header().Get(replayHeader) != "" {
		t.Error("the second person was served the first person's recorded answer")
	}
}

func TestTheKeyIsRequiredAndMustBeAUUID(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want string
	}{
		{"absent", "", "missing_idempotency_key"},
		{"blank", "   ", "missing_idempotency_key"},
		{"not a uuid", "attempt-1", "invalid_idempotency_key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &countingHandler{status: http.StatusAccepted, body: `{}`}
			rec := postWithKey(idempotentRoutes(t, newMemoryStore(), handler), "student-1", tt.key)

			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
			}
			if got := decodeErrorCode(t, rec); got != tt.want {
				t.Errorf("code = %q, want %q", got, tt.want)
			}
			if handler.calls != 0 {
				t.Errorf("the handler ran %d times without a usable key, want 0", handler.calls)
			}
		})
	}
}

// A store that cannot answer fails the request closed, unlike the rate limiter.
//
// Letting the request through would be a coin toss on whether this is a first
// attempt or a retry, and removing that coin toss is the entire purpose of the
// mechanism.
func TestAStoreThatCannotAnswerRefusesTheRequest(t *testing.T) {
	store := newMemoryStore()
	store.claimErr = errors.New("dial tcp: connection refused")

	handler := &countingHandler{status: http.StatusAccepted, body: `{}`}
	rec := postWithKey(idempotentRoutes(t, store, handler), "student-1", testIdempotencyKey)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if handler.calls != 0 {
		t.Errorf("the handler ran %d times without a usable claim, want 0", handler.calls)
	}
}

// Failing to record the answer must not fail the request that produced it.
//
// The ticket is bought. Losing the ability to replay is a real problem for the
// next retry and no reason at all to tell this caller their purchase failed.
func TestAResponseThatCannotBeStoredIsStillReturned(t *testing.T) {
	store := newMemoryStore()
	store.storeErr = errors.New("dial tcp: connection refused")

	handler := &countingHandler{status: http.StatusAccepted, body: `{"purchase_id":"abc"}`}
	rec := postWithKey(idempotentRoutes(t, store, handler), "student-1", testIdempotencyKey)

	if rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	if rec.Body.String() != `{"purchase_id":"abc"}` {
		t.Errorf("body = %q, want the handler's own", rec.Body.String())
	}
}

// The recorded answer is the whole response, so a replay is not a reconstruction.
func TestTheStoredRecordCarriesStatusAndBodyTogether(t *testing.T) {
	store := newMemoryStore()
	handler := &countingHandler{status: http.StatusAccepted, body: `{"purchase_id":"abc"}`}

	postWithKey(idempotentRoutes(t, store, handler), "student-1", testIdempotencyKey)

	raw, ok := store.records[cacheKey("student-1")]
	if !ok {
		t.Fatal("nothing was recorded for the key")
	}

	var stored storedResponse
	if err := json.Unmarshal([]byte(raw[1:]), &stored); err != nil {
		t.Fatalf("decoding the record: %v", err)
	}
	if stored.Status != http.StatusAccepted {
		t.Errorf("stored status = %d, want %d", stored.Status, http.StatusAccepted)
	}
	if string(stored.Body) != `{"purchase_id":"abc"}` {
		t.Errorf("stored body = %s, want the handler's own", stored.Body)
	}
}

// cacheKey mirrors the default namespacing New installs when no key function is
// injected, so a test can look a record up by hand.
func cacheKey(userID string) string {
	return testCampaign + ":" + userID + ":" + testIdempotencyKey
}
