// Package domain defines the core entities used across the wire-inventory service.
package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// ErrNotFound is returned when a requested resource does not exist.
var ErrNotFound = errors.New("not found")

// ProductStatus represents the lifecycle status of a product.
type ProductStatus string

const (
	ProductActive       ProductStatus = "active"
	ProductDiscontinued ProductStatus = "discontinued"
)

// ReservationStatus tracks the current state of a stock reservation.
type ReservationStatus string

const (
	ReservationPending   ReservationStatus = "PENDING"
	ReservationReleased  ReservationStatus = "RELEASED"
	ReservationExpired   ReservationStatus = "EXPIRED"
)

// StockChangeType classifies an entry in the stock_allocations audit log.
type StockChangeType string

const (
	StockReservation StockChangeType = "RESERVATION"
	StockRelease     StockChangeType = "RELEASE"
	StockExpired     StockChangeType = "EXPIRED"
)

// DefaultReservationTTL is how long a PENDING reservation remains valid before
// automatic expiration. Configurable via RESERVATION_TIMEOUT env var.
const DefaultReservationTTL = 15 * time.Minute

// =============================================================================
// Domain entities
// =============================================================================

// Product represents an item in the inventory catalog with current stock levels.
type Product struct {
	ID                uuid.UUID       `json:"id"`
	SKU               string          `json:"sku"`
	Name              string          `json:"name"`
	StockQuantity     int64           `json:"stock_quantity"`
	ReservedQuantity  int64           `json:"reserved_quantity"`
	Status            ProductStatus   `json:"status"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
}

// ToActive constructs a new active product (helper for tests/migrations).
func NewProduct(sku, name string, initialStock int64) *Product {
	return &Product{
		ID:               uuid.New(),
		SKU:              sku,
		Name:             name,
		StockQuantity:    initialStock,
		ReservedQuantity: 0,
		Status:           ProductActive,
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}
}


// AvailableStock calculates currently sellable stock (total minus reserved).
func (p *Product) IsAvailable() bool {
	return p.Status == ProductActive
}

// AvailableStock is stock_quantity minus reserved_quantity — what we can still sell.
func (p *Product) AvailableStock() int64 {
	return p.StockQuantity - p.ReservedQuantity
}

// =============================================================================
// Reservation entity
// =============================================================================

// Reservation represents a temporary hold on stock tied to an order.
type Reservation struct {
	ID        uuid.UUID         `json:"id"`
	ProductID uuid.UUID         `json:"product_id"`
	OrderID   uuid.UUID         `json:"order_id"`
	Quantity  int64             `json:"quantity"`
	Status    ReservationStatus `json:"status"`
	ExpiresAt time.Time         `json:"expires_at"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// HasExpired returns true if the reservation has passed its expires_at time.
func (r *Reservation) HasExpired() bool {
	return time.Now().UTC().After(r.ExpiresAt)
}

// =============================================================================
// StockAllocation entity (audit trail)
// =============================================================================

// StockAllocation is an immutable record of every stock change.
type StockAllocation struct {
	ID            uuid.UUID      `json:"id"`
	ProductID     uuid.UUID      `json:"product_id"`
	ChangeAmount  int64          `json:"change_amount"` // positive = +stock, negative = -stock
	ChangeType    StockChangeType `json:"change_type"`
	ReservationID *uuid.UUID     `json:"reservation_id,omitempty"`
	Note          string         `json:"note,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
}

// =============================================================================
// Request/response DTOs
// =============================================================================

// ReserveRequest is the payload for POST /products/{id}/reserve.
type ReserveRequest struct {
	ProductID uuid.UUID `json:"product_id" validate:"required"`
	OrderID   uuid.UUID `json:"order_id" validate:"required"`
	Quantity  int64     `json:"quantity" validate:"required,min=1"`
}

// ReleaseRequest is the payload for POST /reservations/{id}/release.
type ReleaseRequest struct {
	Reason string `json:"reason,omitempty"`
}

// ListReservationsParams holds query parameters for listing reservations.
type ListReservationsParams struct {
	Status  ReservationStatus `form:"status" url:"status"`
	Limit   int               `form:"limit" url:"limit"`
	Offset  int               `form:"offset" url:"offset"`
}

// ErrorResponse is the standard error envelope returned by all handlers.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
	Code    int    `json:"code"`
}
