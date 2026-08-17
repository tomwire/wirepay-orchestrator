// Package service implements the business logic for wire-order.
package service

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/thomaswire/wpo/cmd/wire-order/repository"
)

var tracer = otel.Tracer("github.com/thomaswire/wpo/cmd/wire-order/service")

// OrderService handles all order business logic.
type OrderService struct {
	repo        repository.OrderRepository
	logger      *zap.Logger
	publisher   OrderPublisher
	inventorySvc InventoryClient
	paymentSvc  PaymentClient
}

// OrderPublisher is the interface for writing events to the transactional outbox.
type OrderPublisher interface {
	WriteToOutbox(ctx context.Context, eventType string, key string, payload interface{}) error
}

// InventoryClient abstracts the inventory reservation service.
type InventoryClient interface {
	ReserveInventory(ctx context.Context, items []repository.OrderItem) error
	ReleaseInventory(ctx context.Context, items []repository.OrderItem) error
}

// PaymentClient abstracts the payment processing service.
type PaymentClient interface {
	Charge(ctx context.Context, orderID string, amount float64, currency string) (string, error)
	Refund(ctx context.Context, chargeID string, amount float64) error
}

// NewOrderService creates a new OrderService with the given dependencies.
func NewOrderService(repo repository.OrderRepository, logger *zap.Logger, publisher OrderPublisher, inventory InventoryClient, payment PaymentClient) *OrderService {
	return &OrderService{repo: repo, logger: logger, publisher: publisher, inventorySvc: inventory, paymentSvc: payment}
}

// NewOrderInput holds the data needed to create a new order.
type NewOrderInput struct {
	ID         uuid.UUID
	Items      []repository.OrderItem
	Currency   string
	ExternalID string
}

// OrderResponse is the structured response returned after order operations.
type OrderResponse struct {
	ID          string    `json:"id"`
	Status      string    `json:"status"`
	TotalAmount float64   `json:"total_amount"`
	Currency    string    `json:"currency"`
	CreatedAt   time.Time `json:"created_at"`
}

// CreateOrder creates a new order and persists it.
func (s *OrderService) CreateOrder(ctx context.Context, input NewOrderInput) (*OrderResponse, error) {
	ctx, span := tracer.Start(ctx, "OrderService.CreateOrder")
	defer span.End()

	if input.Currency == "" {
		input.Currency = "USD"
	}

	order := &repository.Order{
		ID:       input.ID.String(),
		Status:   "CREATED",
		Currency: input.Currency,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	if err := s.repo.Create(ctx, order); err != nil {
		return nil, fmt.Errorf("service: create order: %w", err)
	}

	s.logger.Info("order created", zap.String("order_id", order.ID))

	if s.publisher != nil {
		_ = s.publisher.WriteToOutbox(ctx, "order.created", order.ID, map[string]interface{}{
			"order_id": order.ID,
			"status":   "CREATED",
		})
	}

	return &OrderResponse{
		ID:        order.ID,
		Status:    "CREATED",
		Currency:  input.Currency,
		CreatedAt: order.CreatedAt,
	}, nil
}

// FulfillOrder handles the saga completion flow for an order.
func (s *OrderService) FulfillOrder(ctx context.Context, orderID string) error {
	ctx, span := tracer.Start(ctx, "OrderService.FulfillOrder")
	defer span.End()

	if err := s.repo.UpdateStatus(ctx, orderID, "FULFILLED"); err != nil {
		return fmt.Errorf("service: fulfill order %s: %w", orderID, err)
	}

	s.logger.Info("order fulfilled", zap.String("order_id", orderID))
	return nil
}

// CancelOrder handles the cancellation flow for an order.
func (s *OrderService) CancelOrder(ctx context.Context, orderID string) error {
	ctx, span := tracer.Start(ctx, "OrderService.CancelOrder")
	defer span.End()

	if err := s.repo.UpdateStatus(ctx, orderID, "CANCELLED"); err != nil {
		return fmt.Errorf("service: cancel order %s: %w", orderID, err)
	}

	s.logger.Info("order cancelled", zap.String("order_id", orderID))
	return nil
}

// GetOrder retrieves an order by ID.
func (s *OrderService) GetOrder(ctx context.Context, orderID string) (*repository.Order, error) {
	ctx, span := tracer.Start(ctx, "OrderService.GetOrder")
	defer span.End()

	order, err := s.repo.GetByID(ctx, orderID)
	if err != nil {
		return nil, fmt.Errorf("service: get order %s: %w", orderID, err)
	}

	return order, nil
}
