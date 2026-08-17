// Package handlers provides HTTP handlers for the wire-order service.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gorilla/mux"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/thomaswire/wpo/cmd/wire-order/repository"
	"github.com/thomaswire/wpo/cmd/wire-order/service"
)

var tracer = otel.Tracer("github.com/thomaswire/wpo/cmd/wire-order/handlers")

// Dependencies holds all dependencies required by HTTP handlers.
type Dependencies struct {
	Service *service.OrderService
	Repo    repository.OrderRepository
	Logger  *zap.Logger
}

// OrderRepositoryInterface abstracts the repository for handler access.
type OrderRepositoryInterface interface {
	GetByID(ctx context.Context, id string) (*repository.Order, error)
	UpdateStatus(ctx context.Context, id string, status string) error
}

// WriteError writes a JSON error response.
func writeError(w http.ResponseWriter, logger *zap.Logger, code int, err error, msg string) {
	logger.Error(msg, zap.Error(err))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ============================================================================
// Handler: POST /orders — Create a new order
// ============================================================================

// NewOrderHandler handles POST /orders. Creates a new order from request body.
func NewOrderHandler(deps *Dependencies) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(r.Context(), "handlers.NewOrder")
		defer span.End()

		var input service.NewOrderInput
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeError(w, deps.Logger, http.StatusBadRequest, err, "invalid JSON body")
			return
		}

		if len(input.Items) == 0 {
			writeError(w, deps.Logger, http.StatusBadRequest, errors.New("at least one item required"), "request body must contain at least one order item")
			return
		}

		order, err := deps.Service.CreateOrder(ctx, input)
		if err != nil {
			writeError(w, deps.Logger, http.StatusInternalServerError, err, "failed to create order")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(order)
	}
}

// ============================================================================
// Handler: GET /orders/{id} — Get order by ID
// ============================================================================

// GetOrderHandler handles GET /orders/{id}. Returns the full order record.
func GetOrderHandler(deps *Dependencies) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(r.Context(), "handlers.GetOrder")
		defer span.End()

		vars := mux.Vars(r)
		id := vars["id"]

		if id == "" {
			writeError(w, deps.Logger, http.StatusBadRequest, errors.New("missing order ID"), "order ID is required")
			return
		}

		order, err := deps.Service.GetOrder(ctx, id)
		if err != nil {
			writeError(w, deps.Logger, http.StatusNotFound, err, "order not found with the given ID")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(order)
	}
}

// ============================================================================
// Handler: PUT /orders/{id}/cancel — Cancel an order
// ============================================================================

// CancelOrderHandler handles PUT /orders/{id}/cancel.
func CancelOrderHandler(deps *Dependencies) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(r.Context(), "handlers.CancelOrder")
		defer span.End()

		vars := mux.Vars(r)
		id := vars["id"]

		if id == "" {
			writeError(w, deps.Logger, http.StatusBadRequest, errors.New("missing order ID"), "order ID is required")
			return
		}

		if err := deps.Service.CancelOrder(ctx, id); err != nil {
			writeError(w, deps.Logger, http.StatusInternalServerError, err, "failed to cancel order")
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

// ============================================================================
// Handler: PUT /orders/{id}/fulfill — Fulfill an order (triggers saga completion)
// ============================================================================

// FulfillOrderHandler handles PUT /orders/{id}/fulfill.
func FulfillOrderHandler(deps *Dependencies) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(r.Context(), "handlers.FulfillOrder")
		defer span.End()

		vars := mux.Vars(r)
		id := vars["id"]

		if id == "" {
			writeError(w, deps.Logger, http.StatusBadRequest, errors.New("missing order ID"), "order ID is required")
			return
		}

		if err := deps.Service.FulfillOrder(ctx, id); err != nil {
			writeError(w, deps.Logger, http.StatusInternalServerError, err, "failed to fulfill order")
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

// RegisterRoutes registers all HTTP routes on the given router.
func RegisterRoutes(router *mux.Router, deps *Dependencies) {
	router.HandleFunc("/orders", NewOrderHandler(deps)).Methods("POST")
	router.HandleFunc("/orders/{id}", GetOrderHandler(deps)).Methods("GET")
	router.HandleFunc("/orders/{id}/cancel", CancelOrderHandler(deps)).Methods("PUT")
	router.HandleFunc("/orders/{id}/fulfill", FulfillOrderHandler(deps)).Methods("PUT")
}
