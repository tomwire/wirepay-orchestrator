// Package repository provides PostgreSQL data access for wire-inventory service.
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// Product represents an inventory product.
type Product struct {
	ID                string    `json:"id"`
	SKU               string    `json:"sku"`
	Name              string    `json:"name"`
	StockCount        int64     `json:"stock_count"`
	MinStockThreshold int64     `json:"min_stock_threshold"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// Reservation represents an inventory reservation.
type Reservation struct {
	ID                string    `json:"id"`
	ProductID         string    `json:"product_id"`
	Quantity          int64     `json:"quantity"`
	Status            string    `json:"status"`
	ExpirationHours   int       `json:"expiration_hours"`
	CreatedAt         time.Time `json:"created_at"`
}

// ReservationStatus represents the state of a reservation.
type ReservationStatus string

const (
	ReservationStatusReserved ReservationStatus = "RESERVED"
	ReservationStatusReleased ReservationStatus = "RELEASED"
)

var (
	// ErrNotFound is returned when a record doesn't exist.
	ErrNotFound = errors.New("repository: not found")
	// ErrInsufficientStock indicates stock is too low.
	ErrInsufficientStock = errors.New("repository: insufficient stock")
	// ErrReservationNotFound is returned when a reservation is not found.
	ErrReservationNotFound = errors.New("repository: reservation not found")
)

// InventoryRepo handles PostgreSQL interactions for inventory and reservations.
type InventoryRepo struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// NewInventoryRepo creates a new repository with the given connection pool.
func NewInventoryRepo(pool *pgxpool.Pool, logger *zap.Logger) *InventoryRepo {
	return &InventoryRepo{pool: pool, logger: logger}
}

// CreateProduct inserts a product into inventory and returns it with generated ID.
func (r *InventoryRepo) CreateProduct(ctx context.Context, p *Product) error {
	now := time.Now().UTC()
	const sql = `
		INSERT INTO products (id, sku, name, stock_count, min_stock_threshold, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, sku, name, stock_count, min_stock_threshold, created_at, updated_at
	`

	err := r.pool.QueryRow(ctx, sql, p.ID, p.SKU, p.Name, p.StockCount, p.MinStockThreshold, now, now).Scan(
		&p.ID, &p.SKU, &p.Name, &p.StockCount, &p.MinStockThreshold, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("create product: %w", err)
	}

	return nil
}

// GetProduct retrieves a single product by ID.
func (r *InventoryRepo) GetProduct(ctx context.Context, id string) (*Product, error) {
	var p Product
	err := r.pool.QueryRow(ctx, `
		SELECT id, sku, name, stock_count, min_stock_threshold, created_at, updated_at
		FROM products WHERE id = $1
	`, id).Scan(&p.ID, &p.SKU, &p.Name, &p.StockCount, &p.MinStockThreshold, &p.CreatedAt, &p.UpdatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get product %s: %w", id, err)
	}

	return &p, nil
}

// ReserveStockAtomically performs an atomic reservation via SELECT FOR UPDATE.
func (r *InventoryRepo) ReserveStockAtomically(ctx context.Context, productID string, quantity int64) (*Reservation, error) {
	res := &Reservation{
		ID:        mustUUID().String(),
		ProductID: productID,
		Quantity:  quantity,
		Status:    string(ReservationStatusReserved),
	}

	now := time.Now().UTC()
	const sql = `
		WITH reserved AS (
			UPDATE products
			SET stock_count = stock_count - $2, updated_at = $3
			WHERE id = $1 AND stock_count >= $2
			RETURNING 1
		)
		SELECT 1 FROM reserved
	`

	var success bool
	err := r.pool.QueryRow(ctx, sql, productID, quantity, now).Scan(&success)
	if err != nil {
		return nil, fmt.Errorf("reserve stock: %w", err)
	}

	if !success {
		return nil, ErrInsufficientStock
	}

	insertSQL := `
		INSERT INTO reservations (id, product_id, quantity, status, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	if _, err := r.pool.Exec(ctx, insertSQL, res.ID, productID, quantity, res.Status, time.Now().Add(time.Duration(res.ExpirationHours)*time.Hour), now); err != nil {
		return nil, fmt.Errorf("insert reservation: %w", err)
	}

	return res, nil
}

// ReleaseReservation changes a reservation to Released status.
func (r *InventoryRepo) UpdateReservationStatus(ctx context.Context, id string, status ReservationStatus) error {
	const sql = `UPDATE reservations SET status = $1 WHERE id = $2`

	_, err := r.pool.Exec(ctx, sql, string(status), id)
	if err != nil {
		return fmt.Errorf("update reservation status: %w", err)
	}

	return nil
}

// CheckStockAvailability determines if sufficient stock exists for a product.
func (r *InventoryRepo) CheckStockAvailability(ctx context.Context, productID string, quantity int64) (bool, error) {
	var available bool

	err := r.pool.QueryRow(ctx, `
		SELECT CASE WHEN stock_count >= $2 THEN TRUE ELSE FALSE END
		FROM products WHERE id = $1
	`, productID, quantity).Scan(&available)

	if err != nil {
		return false, fmt.Errorf("check stock availability: %w", err)
	}

	return available, nil
}

func mustUUID() uuid.UUID {
	u, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}
	return u
}
