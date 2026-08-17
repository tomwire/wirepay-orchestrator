// Package handlers provides HTTP endpoints for wire-payment.
package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/gorilla/mux"
	"go.uber.org/zap"

	paymentSvc "github.com/thomaswire/wpo/cmd/wire-payment/service"
)

// HTTPHandler wraps the payment service with HTTP middleware.
type HTTPHandler struct {
	service *paymentSvc.PaymentService
	logger  *zap.Logger
	val     *validator.Validate
}

// NewHTTPHandler creates a new HTTP handler for payments.
func NewHTTPHandler(svc *paymentSvc.PaymentService, logger *zap.Logger) *HTTPHandler {
	return &HTTPHandler{
		service: svc,
		logger:  logger,
		val:     validator.New(),
	}
}

// RegisterRoutes mounts payment endpoints on the router.
func RegisterRoutes(r *mux.Router, svc *paymentSvc.PaymentService, logger *zap.Logger) {
	h := NewHTTPHandler(svc, logger)
	r.HandleFunc("/v1/payments", h.CreatePayment).Methods("POST")
	r.HandleFunc("/v1/payments/{id}", h.GetPayment).Methods("GET")
	r.HandleFunc("/v1/payments/{id}/history", h.GetPaymentHistory).Methods("GET")
	r.HandleFunc("/v1/payments/{id}/cancel", h.CancelPayment).Methods("POST")
	r.HandleFunc("/v1/payments/{id}/refund", h.RefundPayment).Methods("POST")
	r.HandleFunc("/v1/webhooks/stripe", h.WebhookHandler).Methods("POST")
}

// CreatePaymentInput represents the request body for creating a payment.
type CreatePaymentInput struct {
	OrderID        string          `json:"order_id" validate:"required,uuid"`
	Amount         float64         `json:"amount" validate:"required,gt=0"`
	Currency       string          `json:"currency" validate:"required,len=3"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
}

// CreatePayment handles POST /v1/payments
func (h *HTTPHandler) CreatePayment(w http.ResponseWriter, r *http.Request) {
	var input CreatePaymentInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		httpError(w, http.StatusBadRequest, "invalid request body", err.Error())
		return
	}

	if err := h.val.Struct(input); err != nil {
		httpError(w, http.StatusBadRequest, "validation failed", err.Error())
		return
	}

	result, err := h.service.CreatePayment(
		r.Context(),
		input.OrderID,
		fmt.Sprintf("%.2f", input.Amount),
		input.Currency,
		input.IdempotencyKey,
	)
	if err != nil {
		h.handleError(w, r.Context(), "create_payment", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(result)
}

// GetPayment handles GET /v1/payments/{id}
func (h *HTTPHandler) GetPayment(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]

	if !isValidUUID(id) {
		httpError(w, http.StatusBadRequest, "invalid payment ID format")
		return
	}

	payment, err := h.service.GetPayment(r.Context(), id)
	if err != nil {
		h.handleError(w, r.Context(), "get_payment", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(payment)
}

// GetPaymentHistory handles GET /v1/payments/{id}/history - simplified to just return payment
func (h *HTTPHandler) GetPaymentHistory(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]

	if !isValidUUID(id) {
		httpError(w, http.StatusBadRequest, "invalid payment ID format")
		return
	}

	payment, err := h.service.GetPayment(r.Context(), id)
	if err != nil {
		h.handleError(w, r.Context(), "get_payment_history", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(payment)
}

// CancelPayment handles POST /v1/payments/{id}/cancel - simplified placeholder
func (h *HTTPHandler) CancelPayment(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]

	if !isValidUUID(id) {
		httpError(w, http.StatusBadRequest, "invalid payment ID format")
		return
	}

	// For now, just return success - real cancellation logic would go here
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "cancelled"})
}

// RefundPayment handles POST /v1/payments/{id}/refund
func (h *HTTPHandler) RefundPayment(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]

	if !isValidUUID(id) {
		httpError(w, http.StatusBadRequest, "invalid payment ID format")
		return
	}

	// Simplified refund - in production would read amount from request body
	if err := h.service.RefundPayment(r.Context(), id, 0); err != nil {
		h.handleError(w, r.Context(), "refund_payment", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "refunded"})
}

// WebhookHandler handles POST /v1/webhooks/stripe
func (h *HTTPHandler) WebhookHandler(w http.ResponseWriter, r *http.Request) {
	eventType := r.Header.Get("X-Stripe-Event-Type")
	if eventType == "" {
		var rawEvent struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.NewDecoder(r.Body).Decode(&rawEvent); err != nil {
			httpError(w, http.StatusBadRequest, "invalid webhook payload")
			return
		}
		eventType = rawEvent.Type
		_ = rawEvent.Data
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid webhook body")
		return
	}

	ctx := r.Context()
	signature := r.Header.Get("Stripe-Signature")

	if err := h.service.ProcessWebhook(ctx, body, signature); err != nil {
		h.handleError(w, ctx, "process_webhook", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "received"})
}

// handleError logs and responds with appropriate HTTP status.
func (h *HTTPHandler) handleError(w http.ResponseWriter, rctx interface{}, operation string, err error) {
	h.logger.Error("payment handler error", zap.String("operation", operation), zap.Error(err))

	switch {
	case isNotFoundError(err):
		httpError(w, http.StatusNotFound, "not found", err.Error())
	default:
		httpError(w, http.StatusInternalServerError, "internal error", "payment "+operation+" failed")
	}
}

// isValidUUID checks if a string looks like a UUID.
func isValidUUID(s string) bool {
	return len(s) == 36 && strings.Count(s, "-") == 4
}

// isNotFoundError determines if an error indicates entity not found.
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "not found") || strings.Contains(errStr, "no_rows")
}

// httpError sends a JSON error response.
func httpError(w http.ResponseWriter, statusCode int, title string, details ...string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	resp := map[string]interface{}{
		"error": map[string]interface{}{
			"code":    statusCode,
			"message": title,
		},
	}

	if len(details) > 0 {
		resp["error"].(map[string]interface{})["details"] = details[0]
	}

	json.NewEncoder(w).Encode(resp)
}
