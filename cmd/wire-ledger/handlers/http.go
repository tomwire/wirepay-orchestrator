// Package handlers provides HTTP endpoints for wire-ledger.
package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	ledgerSvc "github.com/thomaswire/wpo/cmd/wire-ledger/service"
)

// HTTPHandler wraps the ledger service with HTTP middleware.
type HTTPHandler struct {
	service *ledgerSvc.LedgerService
	logger  *zap.Logger
}

// NewHTTPHandler creates a new HTTP handler for ledger operations.
func NewHTTPHandler(svc *ledgerSvc.LedgerService, logger *zap.Logger) *HTTPHandler {
	return &HTTPHandler{service: svc, logger: logger}
}

// RegisterRoutes mounts ledger endpoints on the router.
func RegisterRoutes(r *mux.Router, svc *ledgerSvc.LedgerService, logger *zap.Logger) {
	h := NewHTTPHandler(svc, logger)
	r.HandleFunc("/health", healthHandler).Methods("GET")
	r.HandleFunc("/ready", readyHandler).Methods("GET")
	r.HandleFunc("/v1/accounts", h.CreateAccount).Methods("POST")
	r.HandleFunc("/v1/accounts/{id}", h.GetAccount).Methods("GET")
	r.HandleFunc("/v1/transactions", h.CreateTransaction).Methods("POST")
	r.HandleFunc("/v1/transactions/{id}", h.GetTransaction).Methods("GET")
	r.HandleFunc("/v1/transactions/{id}/post", h.PostDraftTransaction).Methods("POST")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok","service":"wire-ledger"}`))
}

func readyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ready","service":"wire-ledger"}`))
}

// CreateAccountInput represents the request body for creating an account.
type CreateAccountInput struct {
	Name     string          `json:"name"`
	Type     string          `json:"type"`
	Currency string          `json:"currency"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// CreateAccount handles POST /v1/accounts
func (h *HTTPHandler) CreateAccount(w http.ResponseWriter, r *http.Request) {
	var input CreateAccountInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		httpError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	acct, err := h.service.CreateAccount(r.Context(), ledgerSvc.CreateAccountInput{
		Name:     input.Name,
		Type:     input.Type,
		Currency: input.Currency,
		Metadata: input.Metadata,
	})
	if err != nil {
		h.handleError(w, "create_account", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(acct)
}

// GetAccount handles GET /v1/accounts/{id}
func (h *HTTPHandler) GetAccount(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if !isValidUUID(id) {
		httpError(w, http.StatusBadRequest, "invalid account ID format")
		return
	}

	acct, err := h.service.GetAccount(r.Context(), id)
	if err != nil {
		h.handleError(w, "get_account", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(acct)
}

// CreateTransactionInput represents the request body for creating a transaction.
type CreateTransactionInput struct {
	Status   string          `json:"status"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// CreateTransaction handles POST /v1/transactions
func (h *HTTPHandler) CreateTransaction(w http.ResponseWriter, r *http.Request) {
	var input CreateTransactionInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		httpError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	result, err := h.service.CreateTransaction(r.Context(), ledgerSvc.CreateTransactionInput{
		Status:   input.Status,
		Metadata: input.Metadata,
	})
	if err != nil {
		h.handleError(w, "create_transaction", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(result)
}

// GetTransaction handles GET /v1/transactions/{id}
func (h *HTTPHandler) GetTransaction(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if !isValidUUID(id) {
		httpError(w, http.StatusBadRequest, "invalid transaction ID format")
		return
	}

	tx, err := h.service.GetTransaction(r.Context(), id)
	if err != nil {
		h.handleError(w, "get_transaction", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tx)
}

// PostDraftTransaction handles POST /v1/transactions/{id}/post
func (h *HTTPHandler) PostDraftTransaction(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if !isValidUUID(id) {
		httpError(w, http.StatusBadRequest, "invalid transaction ID format")
		return
	}

	if err := h.service.PostTransaction(r.Context(), id); err != nil {
		h.handleError(w, "post_draft", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "posted"})
}

// handleError logs and responds with appropriate HTTP status.
func (h *HTTPHandler) handleError(w http.ResponseWriter, operation string, err error) {
	h.logger.Error("ledger handler error", zap.String("operation", operation), zap.Error(err))
	httpError(w, http.StatusInternalServerError, "internal error")
}

// isValidUUID checks if a string looks like a UUID.
func isValidUUID(s string) bool {
	return len(s) == 36 && strings.Count(s, "-") == 4
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
