// Package repository provides PostgreSQL data access for the wire-order service.
package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Order represents an order entity.
type Order struct {
	ID               string          `json:"id"`
	CustomerID       string          `json:"customer_id"`
	Status           string          `json:"status"`
	TotalAmountCents int64           `json:"total_amount_cents"`
	Currency         string          `json:"currency"`
	Items            json.RawMessage `json:"items"`
	ShippingAddress  *string         `json:"shipping_address,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

// CheckoutSession represents a checkout session linked to an order.
type CheckoutSession struct {
	ID           string    `json:"id"`
	OrderID      string    `json:"order_id"`
	Status       string    `json:"status"`
	PaymentToken *string   `json:"payment_token,omitempty"`
	ExpiresAt    time.Time `json:"expires_at"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// OrderWithPayment is an order joined with its payment info.
type OrderWithPayment struct {
	Order   Order         `json:"order"`
	Payment PaymentInfo   `json:"payment,omitempty"`
}

// PaymentInfo holds related payment data.
type PaymentInfo struct {
	ID          string  `json:"id"`
	Status      string  `json:"status"`
	ProviderID  *string `json:"provider_payment_id,omitempty"`
	Attempts    int     `json:"attempt_count"`
	Metadata    *string `json:"metadata,omitempty"`
}

// OrderItem represents an item in the order.
type OrderItem struct {
	Name  string `json:"name"`
	SKU   string `json:"sku"`
	Price int64  `json:"price_cents"`
	Qty   int    `json:"quantity"`
}

var (
	// ErrNotFound is returned when a record doesn't exist.
	ErrNotFound = errors.New("repository: not found")
)

// OrderRepository defines the interface for order data access.
type OrderRepository interface {
	Create(ctx context.Context, order *Order) error
	GetByID(ctx context.Context, id string) (*Order, error)
	UpdateStatus(ctx context.Context, id string, status string) error
	UpsertCheckoutSession(ctx context.Context, session *CheckoutSession) error
	GetOrderWithPayment(ctx context.Context, id string) (*OrderWithPayment, error)
}

// PostgresOrderRepo implements the OrderRepository interface using PostgreSQL.
type PostgresOrderRepo struct {
	pool *pgxpool.Pool
}

// NewPostgresOrderRepo creates a new repository with the given connection pool.
func NewPostgresOrderRepo(pool *pgxpool.Pool) *PostgresOrderRepo {
	return &PostgresOrderRepo{pool: pool}
}

// Create inserts a new order into the database and returns its full representation.
func (r *PostgresOrderRepo) Create(ctx context.Context, order *Order) error {
	if order.ID == "" {
		orderID, err := uuid.NewV4()
		if err != nil {
			return fmt.Errorf("generate order UUID: %w", err)
		}
		order.ID = orderID.String()
	}

	now := time.Now().UTC()
	const sql = `
		INSERT INTO orders (id, customer_id, status, total_amount_cents, currency,
			items, shipping_address, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id, customer_id, status, total_amount_cents, currency,
		          items, shipping_address, created_at, updated_at
	`

	var itemsStr string
	err := r.pool.QueryRow(ctx, sql,
		order.ID, order.CustomerID, order.Status, order.TotalAmountCents, order.Currency,
		string(order.Items), order.ShippingAddress, now, now,
	).Scan(
		&order.ID, &order.CustomerID, &order.Status, &order.TotalAmountCents, &order.Currency,
		&itemsStr,
		&order.ShippingAddress,
		&order.CreatedAt, &order.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("create order: %w", err)
	}

	order.Items = json.RawMessage(itemsStr)
	return nil
}

// GetByID retrieves an order by its UUID. Returns ErrNotFound if not found.
func (r *PostgresOrderRepo) GetByID(ctx context.Context, id string) (*Order, error) {
	var o Order
	var itemsJSON json.RawMessage

	err := r.pool.QueryRow(ctx, `
		SELECT id, customer_id, status, total_amount_cents, currency, items, shipping_address, created_at, updated_at
		FROM orders WHERE id = $1
	`, id).Scan(
		&o.ID, &o.CustomerID, &o.Status, &o.TotalAmountCents, &o.Currency,
		&itemsJSON,
		&o.ShippingAddress,
		&o.CreatedAt, &o.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get order %s: %w", id, err)
	}

	o.Items = itemsJSON
	return &o, nil
}

// UpdateStatus changes an order's status and refreshes updated_at.
func (r *PostgresOrderRepo) UpdateStatus(ctx context.Context, id string, status string) error {
	if id == "" {
		return fmt.Errorf("repository: update order status — id is empty")
	}
	if status == "" {
		return fmt.Errorf("repository: update order status — status is empty")
	}

	now := time.Now().UTC()
	const sql = `UPDATE orders SET status = $1, updated_at = $2 WHERE id = $3`

	_, err := r.pool.Exec(ctx, sql, status, now, id)
	if err != nil {
		return fmt.Errorf("repository: update order %s status to %q: %w", id, status, err)
	}

	return nil
}

// UpsertCheckoutSession inserts or updates a checkout session for an order.
func (r *PostgresOrderRepo) UpsertCheckoutSession(ctx context.Context, session *CheckoutSession) error {
	if session == nil {
		return fmt.Errorf("repository: upsert checkout session — session is nil")
	}

	var existing CheckoutSession
	err := r.pool.QueryRow(ctx, `
		SELECT id, order_id, status, payment_token, expires_at, created_at, updated_at
		FROM checkout_sessions WHERE order_id = $1 AND status IN ('ACTIVE', 'PENDING')
		LIMIT 1
	`, session.OrderID).Scan(
		&existing.ID, &existing.OrderID, &existing.Status, &existing.PaymentToken,
		&existing.ExpiresAt, &existing.CreatedAt, &existing.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return r.insertCheckoutSession(ctx, session)
	}
	if err != nil {
		return fmt.Errorf("repository: check existing checkout session for order %s: %w", session.OrderID, err)
	}

	session.ID = existing.ID
	return r.updateCheckoutSession(ctx, session)
}

// insertCheckoutSession performs an INSERT for a new checkout session row.
func (r *PostgresOrderRepo) insertCheckoutSession(ctx context.Context, session *CheckoutSession) error {
	now := time.Now().UTC()
	const sql = `
		INSERT INTO checkout_sessions (id, order_id, status, payment_token, expires_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	_, err := r.pool.Exec(ctx, sql, session.ID, session.OrderID, session.Status, session.PaymentToken, session.ExpiresAt, now, now)
	if err != nil {
		return fmt.Errorf("repository: insert checkout session for order %s: %w", session.OrderID, err)
	}

	return nil
}

// updateCheckoutSession performs an UPDATE for an existing checkout session.
func (r *PostgresOrderRepo) updateCheckoutSession(ctx context.Context, session *CheckoutSession) error {
	now := time.Now().UTC()
	const sql = `
		UPDATE checkout_sessions SET status = $1, payment_token = $2, expires_at = $3, updated_at = $4
		WHERE id = $5 AND order_id = $6
	`

	_, err := r.pool.Exec(ctx, sql, session.Status, session.PaymentToken, session.ExpiresAt, now, session.ID, session.OrderID)
	if err != nil {
		return fmt.Errorf("repository: update checkout session %s: %w", session.ID, err)
	}

	return nil
}

// GetOrderWithPayment retrieves an order along with its related payment info if any.
func (r *PostgresOrderRepo) GetOrderWithPayment(ctx context.Context, id string) (*OrderWithPayment, error) {
	var o OrderWithPayment
	var itemsJSON json.RawMessage

	err := r.pool.QueryRow(ctx, `
		SELECT o.id, o.customer_id, o.status, o.total_amount_cents, o.currency, o.items, o.shipping_address,
		       o.created_at, o.updated_at,
		       p.id as payment_id, p.status as payment_status, p.provider_payment_id,
		       p.attempt_count as payment_attempts, p.metadata as payment_metadata
		FROM orders o LEFT JOIN payments p ON o.id = p.order_id WHERE o.id = $1
	`, id).Scan(
		&o.Order.ID, &o.Order.CustomerID, &o.Order.Status, &o.Order.TotalAmountCents, &o.Order.Currency,
		&itemsJSON,
		&o.Order.ShippingAddress,
		&o.Order.CreatedAt, &o.Order.UpdatedAt,
		&o.Payment.ID, &o.Payment.Status, o.Payment.ProviderID,
		&o.Payment.Attempts, o.Payment.Metadata,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get order with payment %s: %w", id, err)
	}

	o.Order.Items = itemsJSON
	return &o, nil
}
