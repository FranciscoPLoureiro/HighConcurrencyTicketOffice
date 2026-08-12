package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/correlation"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/health"
)

const testCampaign = "queima-2026"

// stubPurchaser returns whatever the test needs, so the handler's mapping from
// domain outcome to HTTP response can be checked without a database.
type stubPurchaser struct {
	purchase domain.Purchase
	err      error
	readErr  error
	// calls records what the handler passed down, which is how the test
	// catches a handler that sells the wrong campaign or loses the user.
	calls []purchaseCall
}

type purchaseCall struct{ campaignID, userID, idempotencyKey string }

func (s *stubPurchaser) Purchase(_ context.Context, campaignID, userID, idempotencyKey string) (domain.Purchase, error) {
	s.calls = append(s.calls, purchaseCall{campaignID, userID, idempotencyKey})
	return s.purchase, s.err
}

func (s *stubPurchaser) ReadPurchase(_ context.Context, _, _ string) (domain.Purchase, error) {
	return s.purchase, s.readErr
}

func purchaseRoutes(p Purchaser) http.Handler {
	return New(Config{
		Health:     health.New(),
		Purchaser:  p,
		CampaignID: testCampaign,
		Logger:     slog.New(slog.DiscardHandler),
	}).Routes()
}

func postPurchase(t *testing.T, handler http.Handler, userID string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tickets/purchase", nil)
	if userID != "" {
		req.Header.Set(userIDHeader, userID)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func getStatus(handler http.Handler, userID, purchaseID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/tickets/"+purchaseID+"/status", nil)
	req.Header.Set(userIDHeader, userID)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// decodeErrorCode pulls the machine-readable code out of a failure body. The
// code is the contract; the prose alongside it is not.
func decodeErrorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()

	var body errorBody
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decoding error body: %v", err)
	}
	return body.Error.Code
}

// A sale is answered with 202, not 200.
//
// The decision is made and durably recorded; the document is not, and will not
// be for another two seconds. 200 would promise a ticket that does not exist
// yet, which is exactly the confusion asynchronous fulfilment introduces and
// the status code is there to prevent.
func TestASuccessfulPurchaseIsAcceptedRatherThanCompleted(t *testing.T) {
	stub := &stubPurchaser{purchase: domain.Purchase{
		ID:         "b0a1c2d3-0000-4000-8000-000000000000",
		CampaignID: testCampaign,
		UserID:     "student-1",
		Status:     domain.StatusPending,
	}}

	rec := postPurchase(t, purchaseRoutes(stub), "student-1")

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusAccepted, rec.Body)
	}

	var body acceptedResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	if body.PurchaseID != stub.purchase.ID {
		t.Errorf("purchase_id = %q, want %q", body.PurchaseID, stub.purchase.ID)
	}
	if body.Status != string(domain.StatusPending) {
		t.Errorf("status = %q, want %q", body.Status, domain.StatusPending)
	}

	// The caller is told where to watch, in the body and in the header, and
	// the two have to agree — a Location pointing somewhere the JSON does not
	// is a client bug waiting to be written.
	want := "/api/v1/tickets/" + stub.purchase.ID + "/status"
	if body.StatusURL != want {
		t.Errorf("status_url = %q, want %q", body.StatusURL, want)
	}
	if got := rec.Header().Get("Location"); got != want {
		t.Errorf("Location = %q, want %q", got, want)
	}

	// And with an identifier they can quote when something goes wrong.
	if body.CorrelationID == "" {
		t.Error("the response carries no correlation id")
	}
	if got := rec.Header().Get(correlation.HeaderName); got != body.CorrelationID {
		t.Errorf("%s header = %q, want the body's %q", correlation.HeaderName, got, body.CorrelationID)
	}
}

// A caller-supplied correlation id is honoured, so a trace that started
// upstream carries on rather than restarting at this service.
func TestASuppliedCorrelationIDIsKept(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tickets/purchase", nil)
	req.Header.Set(userIDHeader, "student-1")
	req.Header.Set(correlation.HeaderName, "trace-from-the-gateway")

	rec := httptest.NewRecorder()
	purchaseRoutes(&stubPurchaser{}).ServeHTTP(rec, req)

	if got := rec.Header().Get(correlation.HeaderName); got != "trace-from-the-gateway" {
		t.Errorf("%s = %q, want the value the caller sent", correlation.HeaderName, got)
	}
}

func TestTheStatusEndpointReportsThePurchase(t *testing.T) {
	stub := &stubPurchaser{purchase: domain.Purchase{
		ID:         "b0a1c2d3-0000-4000-8000-000000000000",
		CampaignID: testCampaign,
		UserID:     "student-1",
		Status:     domain.StatusConfirmed,
	}}

	rec := getStatus(purchaseRoutes(stub), "student-1", stub.purchase.ID)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusOK, rec.Body)
	}

	var body statusResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decoding body: %v", err)
	}
	if body.Status != string(domain.StatusConfirmed) {
		t.Errorf("status = %q, want %q", body.Status, domain.StatusConfirmed)
	}
}

// Somebody else's purchase is reported as missing, not as forbidden.
//
// "This exists but is not yours" confirms that a guessed identifier is real,
// which is a slow but perfectly good way to enumerate other people's tickets.
func TestOnePersonCannotReadAnothersPurchase(t *testing.T) {
	stub := &stubPurchaser{purchase: domain.Purchase{
		ID:         "b0a1c2d3-0000-4000-8000-000000000000",
		CampaignID: testCampaign,
		UserID:     "student-1",
		Status:     domain.StatusConfirmed,
	}}

	rec := getStatus(purchaseRoutes(stub), "student-2", stub.purchase.ID)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if got := decodeErrorCode(t, rec); got != "purchase_not_found" {
		t.Errorf("code = %q, want purchase_not_found", got)
	}
}

func TestAMissingPurchaseIsReportedAsSuch(t *testing.T) {
	stub := &stubPurchaser{readErr: domain.ErrPurchaseNotFound}

	rec := getStatus(purchaseRoutes(stub), "student-1", "b0a1c2d3-0000-4000-8000-000000000000")

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// The handler must sell the campaign it was configured with and attribute the
// purchase to the caller in the header — not to anything taken from the body.
func TestThePurchaseIsAttributedToTheCallerAndTheConfiguredCampaign(t *testing.T) {
	stub := &stubPurchaser{}

	postPurchase(t, purchaseRoutes(stub), "student-42")

	if len(stub.calls) != 1 {
		t.Fatalf("purchaser called %d times, want 1", len(stub.calls))
	}
	if stub.calls[0].userID != "student-42" {
		t.Errorf("user = %q, want student-42", stub.calls[0].userID)
	}
	if stub.calls[0].campaignID != testCampaign {
		t.Errorf("campaign = %q, want %q", stub.calls[0].campaignID, testCampaign)
	}
}

// Each refusal gets its own code so a client can act on the difference without
// reading the message, which is the thing most likely to be reworded.
func TestEachRefusalMapsToItsOwnStatusAndCode(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{"sold out", domain.ErrSoldOut, http.StatusConflict, "stock_exhausted"},
		{"already bought", domain.ErrAlreadyPurchased, http.StatusConflict, "already_purchased"},
		{"no such campaign", domain.ErrCampaignNotFound, http.StatusNotFound, "campaign_not_found"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := postPurchase(t, purchaseRoutes(&stubPurchaser{err: tc.err}), "student-1")

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if got := decodeErrorCode(t, rec); got != tc.wantCode {
				t.Errorf("code = %q, want %q", got, tc.wantCode)
			}
		})
	}
}

// Two refusals share a status code, so the code is the only thing telling them
// apart. Losing that distinction would be invisible to a status-code assertion.
func TestTheTwoConflictsAreDistinguishable(t *testing.T) {
	soldOut := decodeErrorCode(t, postPurchase(t, purchaseRoutes(&stubPurchaser{err: domain.ErrSoldOut}), "a"))
	already := decodeErrorCode(t, postPurchase(t, purchaseRoutes(&stubPurchaser{err: domain.ErrAlreadyPurchased}), "b"))

	if soldOut == already {
		t.Errorf("both conflicts report code %q; a client cannot tell them apart", soldOut)
	}
}

// An unexpected failure is a bug or an outage. Either way the detail is useless
// to the caller and can say more about the system than it should.
func TestAnUnexpectedFailureDoesNotLeakItsCause(t *testing.T) {
	stub := &stubPurchaser{err: errors.New("pq: relation \"purchases\" does not exist")}

	rec := postPurchase(t, purchaseRoutes(stub), "student-1")

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if body := rec.Body.String(); strings.Contains(body, "relation") {
		t.Errorf("response body leaks the underlying error: %s", body)
	}
}

func TestARequestWithoutAnIdentityIsRejected(t *testing.T) {
	stub := &stubPurchaser{}

	rec := postPurchase(t, purchaseRoutes(stub), "")

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if got := decodeErrorCode(t, rec); got != "missing_identity" {
		t.Errorf("code = %q, want missing_identity", got)
	}
	// Nothing should have reached the purchaser.
	if len(stub.calls) != 0 {
		t.Errorf("purchaser was called %d times for an unauthenticated request", len(stub.calls))
	}
}

func TestABlankIdentityIsRejected(t *testing.T) {
	stub := &stubPurchaser{}

	rec := postPurchase(t, purchaseRoutes(stub), "   ")

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d for a whitespace-only identity", rec.Code, http.StatusUnauthorized)
	}
	if len(stub.calls) != 0 {
		t.Error("a whitespace-only identity reached the purchaser")
	}
}

// An unbounded header flows straight into a database query and a Redis set
// member on the hottest path in the system.
func TestAnOversizedIdentityIsRejected(t *testing.T) {
	stub := &stubPurchaser{}
	oversized := make([]byte, maxUserIDLength+1)
	for i := range oversized {
		oversized[i] = 'a'
	}

	rec := postPurchase(t, purchaseRoutes(stub), string(oversized))

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if len(stub.calls) != 0 {
		t.Error("an oversized identity reached the purchaser")
	}
}

func TestPurchaseRejectsNonPOSTMethods(t *testing.T) {
	rec := httptest.NewRecorder()
	purchaseRoutes(&stubPurchaser{}).ServeHTTP(
		rec, httptest.NewRequest(http.MethodGet, "/api/v1/tickets/purchase", nil))

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}
