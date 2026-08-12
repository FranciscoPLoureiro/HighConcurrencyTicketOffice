package httpapi

import (
	"errors"
	"net/http"
	"time"

	"github.com/FranciscoPLoureiro/HighConcurrencyTicketOffice/internal/domain"
)

// purchaseResponse is what a successful purchase returns.
type purchaseResponse struct {
	PurchaseID string    `json:"purchase_id"`
	CampaignID string    `json:"campaign_id"`
	UserID     string    `json:"user_id"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
}

func (s *Server) handlePurchase(w http.ResponseWriter, r *http.Request) {
	userID, ok := userIDFrom(r.Context())
	if !ok {
		// Only reachable if the route is wired without the middleware.
		// Failing loudly beats attributing the purchase to nobody.
		s.writeInternalError(w, "purchase handler reached without an identity", nil)
		return
	}

	purchase, err := s.purchaser.Purchase(r.Context(), s.campaignID, userID,
		r.Header.Get(idempotencyKeyHeader))

	switch {
	case errors.Is(err, domain.ErrSoldOut):
		// 409 rather than 404 or 410: the campaign exists and the request
		// was well formed, it just lost the race for a finite resource.
		writeError(w, http.StatusConflict, "stock_exhausted",
			"there are no tickets left for this campaign")

	case errors.Is(err, domain.ErrAlreadyPurchased):
		writeError(w, http.StatusConflict, "already_purchased",
			"this account already holds a ticket for this campaign")

	case errors.Is(err, domain.ErrCampaignNotFound):
		writeError(w, http.StatusNotFound, "campaign_not_found",
			"no such campaign")

	case err != nil:
		s.writeInternalError(w, "purchase failed", err)

	default:
		s.writeJSON(w, http.StatusOK, purchaseResponse{
			PurchaseID: purchase.ID,
			CampaignID: purchase.CampaignID,
			UserID:     purchase.UserID,
			Status:     string(purchase.Status),
			CreatedAt:  purchase.CreatedAt,
		})
	}
}
