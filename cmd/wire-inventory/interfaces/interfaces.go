// Package interfaces defines contracts for dependency injection, enabling testability
// and decoupling between the service layer, repository, and external dependencies.
package interfaces

import (
	"context"
	"time"

	"github.com/thomaswire/wpo/cmd/wire-inventory/domain"
)

// Repository defines the data access contract for inventory operations.
type Repository interface {
	GetProductByID(ctx context.Context, id string) (*domain.Product, error)
	CreateReservation(ctx context.Context, reservation *domain.Reservation) error
	UpdateReservationStatus(ctx context.Context, id string, status domain.ReservationStatus) error
	ReserveStockAtomically(ctx context.Context, productID string, quantity int64) error
	GetReservationsByStatus(ctx context.Context, status domain.ReservationStatus, limit, offset int) ([]*domain.Reservation, error)
	ListExpiredReservations(ctx context.Context) ([]*domain.Reservation, error)
	MarkProductDiscontinued(ctx context.Context, productID string) error
}

// LockProvider abstracts the distributed locking mechanism for stock operations.
type LockProvider interface {
	Acquire(ctx context.Context, timeout time.Duration) error
	Release() error
	HasOwnership() bool
}
