// Package service implements the business logic for wire-inventory.
package service

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/thomaswire/wpo/cmd/wire-inventory/repository"
	redisshared "github.com/thomaswire/wpo/pkg/shared/redis"
	wpokat "github.com/thomaswire/wpo/pkg/shared/kafka"
)

var tracer = otel.Tracer("github.com/thomaswire/wpo/cmd/wire-inventory/service")

// InventoryService handles all inventory business logic and stock management.
type InventoryService struct {
	repo          *repository.InventoryRepo
	logger        *zap.Logger
	redisClient   *redisshared.ClientWrapper
	kafkaProducer *wpokat.Producer
}

// NewInventoryService creates a new InventoryService with the given dependencies.
func NewInventoryService(repo *repository.InventoryRepo, logger *zap.Logger, redisClient *redisshared.ClientWrapper, kafkaProducer *wpokat.Producer) *InventoryService {
	return &InventoryService{repo: repo, logger: logger, redisClient: redisClient, kafkaProducer: kafkaProducer}
}

// CreateProductInput holds the data needed to create a new product.
type CreateProductInput struct {
	SKU               string `json:"sku"`
	Name              string `json:"name"`
	StockCount        int64  `json:"stock_count"`
	MinStockThreshold int64  `json:"min_stock_threshold"`
}

// ProductResponse is the structured response returned after product operations.
type ProductResponse struct {
	ID                string    `json:"id"`
	SKU               string    `json:"sku"`
	Name              string    `json:"name"`
	StockCount        int64     `json:"stock_count"`
	MinStockThreshold int64     `json:"min_stock_threshold"`
	CreatedAt         time.Time `json:"created_at"`
}

// CreateProduct creates a new product and persists it.
func (s *InventoryService) CreateProduct(ctx context.Context, input CreateProductInput) (*ProductResponse, error) {
	ctx, span := tracer.Start(ctx, "InventoryService.CreateProduct")
	defer span.End()

	product := &repository.Product{
		SKU:               input.SKU,
		Name:              input.Name,
		StockCount:        input.StockCount,
		MinStockThreshold: input.MinStockThreshold,
		CreatedAt:         time.Now().UTC(),
		UpdatedAt:         time.Now().UTC(),
	}

	if err := s.repo.CreateProduct(ctx, product); err != nil {
		return nil, fmt.Errorf("service: create product: %w", err)
	}

	s.logger.Info("product created", zap.String("id", product.ID))

	return &ProductResponse{
		ID:                product.ID,
		SKU:               product.SKU,
		Name:              product.Name,
		StockCount:        product.StockCount,
		MinStockThreshold: product.MinStockThreshold,
		CreatedAt:         product.CreatedAt,
	}, nil
}

// GetProduct retrieves a product by ID.
func (s *InventoryService) GetProduct(ctx context.Context, productID string) (*repository.Product, error) {
	ctx, span := tracer.Start(ctx, "InventoryService.GetProduct")
	defer span.End()

	product, err := s.repo.GetProduct(ctx, productID)
	if err != nil {
		return nil, fmt.Errorf("service: get product %s: %w", productID, err)
	}

	return product, nil
}

// ReserveStockAtomically performs an atomic stock reservation via SELECT FOR UPDATE.
func (s *InventoryService) ReserveStockAtomically(ctx context.Context, productID string, quantity int64) (*repository.Reservation, error) {
	ctx, span := tracer.Start(ctx, "InventoryService.ReserveStock")
	defer span.End()

	reservation, err := s.repo.ReserveStockAtomically(ctx, productID, quantity)
	if err != nil {
		return nil, fmt.Errorf("service: reserve stock for %s (qty=%d): %w", productID, quantity, err)
	}

	s.logger.Info("stock reserved", zap.String("product_id", productID), zap.Int64("quantity", quantity))

	if s.kafkaProducer != nil {
		payload := map[string]interface{}{
			"reservation_id": reservation.ID,
			"product_id":     productID,
			"quantity":       quantity,
			"status":         string(repository.ReservationStatusReserved),
		}
		key := []byte(productID)
		eventType := fmt.Sprintf("inventory.%s", "stock_reserved")
		topic := eventType
		_ = s.kafkaProducer.Publish(ctx, topic, key, payload)
	}

	return reservation, nil
}

// ReleaseStock releases a reserved stock allocation.
func (s *InventoryService) ReleaseStock(ctx context.Context, reservationID string) error {
	ctx, span := tracer.Start(ctx, "InventoryService.ReleaseStock")
	defer span.End()

	if err := s.repo.UpdateReservationStatus(ctx, reservationID, repository.ReservationStatusReleased); err != nil {
		return fmt.Errorf("service: release stock for %s: %w", reservationID, err)
	}

	s.logger.Info("stock released", zap.String("reservation_id", reservationID))

	if s.kafkaProducer != nil {
		key := []byte(reservationID)
		topic := "inventory.stock_released"
		payload := map[string]interface{}{
			"reservation_id": reservationID,
		}
		_ = s.kafkaProducer.Publish(ctx, topic, key, payload)
	}

	return nil
}

// CheckStockAvailability determines if sufficient stock exists for a product.
func (s *InventoryService) CheckStockAvailability(ctx context.Context, productID string, quantity int64) (bool, error) {
	ctx, span := tracer.Start(ctx, "InventoryService.CheckStock")
	defer span.End()

	available, err := s.repo.CheckStockAvailability(ctx, productID, quantity)
	if err != nil {
		return false, fmt.Errorf("service: check stock for %s: %w", productID, err)
	}

	s.logger.Info("stock availability checked", zap.String("product_id", productID), zap.Bool("available", available))
	return available, nil
}

// Ensure uuid import is used.
func _useUUID() {
	_, _ = uuid.NewV4()
}
