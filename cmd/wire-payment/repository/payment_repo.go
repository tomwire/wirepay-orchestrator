// Package repository provides PostgreSQL data access for wire-payment service.
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

// Payment represents a payment entity.
type Payment struct {
	ID           string   `json:"id"`
	OrderID      string   `json:"order_id"`
	Status       string   `json:"status"`
	ProviderID   *string  `json:"provider_payment_id,omitempty"`
	AmountCents  int64    `json:"amount_cents"`
	Currency     string   `json:"currency"`
	Metadata     *string  `json:"metadata,omitempty"`
	AttemptCount int      `json:"attempt_count"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

var (
	// ErrNotFound is returned when a record doesn't exist.
	ErrNotFound = errors.New("repository: not found")
)

// PaymentRepo handles PostgreSQL interactions for payments and refunds.
type PaymentRepo struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// NewPaymentRepo creates a new repository with the given connection pool.
func NewPaymentRepo(pool *pgxpool.Pool, logger *zap.Logger) *PaymentRepo {
	return &PaymentRepo{pool: pool, logger: logger}
}

// Create creates a new payment record and returns it with generated ID.
func (r *PaymentRepo) Create(ctx context.Context, p *Payment) error {
	if p.ID == "" {
		pID, err := uuid.NewV4()
		if err != nil {
			return fmt.Errorf("generate payment UUID: %w", err)
		}
		p.ID = pID.String()
	}

	now := time.Now().UTC()
	const sql = `
		INSERT INTO payments (id, order_id, status, provider_payment_id, amount_cents, currency,
			metadata, attempt_count, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id, order_id, status, provider_payment_id, amount_cents, currency,
		          metadata, attempt_count, created_at, updated_at
	`

	err := r.pool.QueryRow(ctx, sql,
		p.ID, p.OrderID, p.Status, p.ProviderID, p.AmountCents, p.Currency,
		p.Metadata, p.AttemptCount, now, now,
	).Scan(
		&p.ID, &p.OrderID, &p.Status, &p.ProviderID, &p.AmountCents, &p.Currency,
		&p.Metadata, &p.AttemptCount, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("create payment: %w", err)
	}

	return nil
}

// GetByID retrieves a payment by ID.
func (r *PaymentRepo) GetByID(ctx context.Context, id string) (*Payment, error) {
	var p Payment

	err := r.pool.QueryRow(ctx, `
		SELECT id, order_id, status, provider_payment_id, amount_cents, currency,
		       metadata, attempt_count, created_at, updated_at
		FROM payments WHERE id = $1
	`, id).Scan(
		&p.ID, &p.OrderID, &p.Status, &p.ProviderID, &p.AmountCents, &p.Currency,
		&p.Metadata, &p.AttemptCount, &p.CreatedAt, &p.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get payment %s: %w", id, err)
	}

	return &p, nil
}

// GetByOrderID retrieves a payment by order ID.
func (r *PaymentRepo) GetByOrderID(ctx context.Context, orderID string) (*Payment, error) {
	var p Payment

	err := r.pool.QueryRow(ctx, `
		SELECT id, order_id, status, provider_payment_id, amount_cents, currency,
		       metadata, attempt_count, created_at, updated_at
		FROM payments WHERE order_id = $1
		LIMIT 1
	`, orderID).Scan(
		&p.ID, &p.OrderID, &p.Status, &p.ProviderID, &p.AmountCents, &p.Currency,
		&p.Metadata, &p.AttemptCount, &p.CreatedAt, &p.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get payment for order %s: %w", orderID, err)
	}

	return &p, nil
}

// UpdateStatus updates a payment's status and increments attempt count if retrying.
func (r *PaymentRepo) UpdateStatus(ctx context.Context, id string, status string) error {
	now := time.Now().UTC()
	const sql = `UPDATE payments SET status = $1, updated_at = $2 WHERE id = $3`

	_, err := r.pool.Exec(ctx, sql, status, now, id)
	if err != nil {
		return fmt.Errorf("update payment %s status to %q: %w", id, status, err)
	}

	return nil
}

// IncrementAttempts increments the attempt count for a payment.
func (r *PaymentRepo) IncrementAttempts(ctx context.Context, id string) error {
	const sql = `UPDATE payments SET attempt_count = attempt_count + 1 WHERE id = $1`

	_, err := r.pool.Exec(ctx, sql, id)
	if err != nil {
		return fmt.Errorf("increment payment attempts for %s: %w", id, err)
	}

	return nil
}

// UpdateProviderID sets the provider payment ID on a payment.
func (r *PaymentRepo) UpdateProviderID(ctx context.Context, id string, providerID string) error {
	const sql = `UPDATE payments SET provider_payment_id = $1, updated_at = $2 WHERE id = $3`

	now := time.Now().UTC()
	_, err := r.pool.Exec(ctx, sql, &providerID, now, id)
	if err != nil {
		return fmt.Errorf("update payment %s provider ID: %w", id, err)
	}

	return nil
}

// GetByProviderID retrieves a payment by provider payment ID (e.g., Stripe PaymentIntent ID).
func (r *PaymentRepo) GetByProviderID(ctx context.Context, providerID string) (*Payment, error) {
	var p Payment

	err := r.pool.QueryRow(ctx, `
		SELECT id, order_id, status, provider_payment_id, amount_cents, currency,
		       metadata, attempt_count, created_at, updated_at
		FROM payments WHERE provider_payment_id = $1
		LIMIT 1
	`, providerID).Scan(
		&p.ID, &p.OrderID, &p.Status, &p.ProviderID, &p.AmountCents, &p.Currency,
		&p.Metadata, &p.AttemptCount, &p.CreatedAt, &p.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get payment by provider ID %s: %w", providerID, err)
	}

	return &p, nil
}
