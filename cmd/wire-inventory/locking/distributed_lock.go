// Package locking provides a Redlock wrapper that serializes stock operations across
// multiple service instances. The lock scope is per-product, allowing concurrent
// reservations on different products while blocking oversell races on the same product.
package locking

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	redisShared "github.com/thomaswire/wpo/pkg/shared/redis"
)

// RedlockWrapper wraps the shared Redlock with a product-scoped key and context propagation.
type RedlockWrapper struct {
	lock      *redisShared.Redlock
	productID string
	logger    *zap.Logger
	tracer    trace.Tracer
	owned     bool // true when we successfully acquired and have not released
}

// lockKey returns the Redis key used for this product's distributed lock.
func lockKey(productID string) string {
	return fmt.Sprintf("inventory:lock:product:%s", productID)
}

// NewProductLock creates a RedlockWrapper scoped to the given product.
// The lock TTL defaults to 30 seconds — sufficient for a single stock operation
// but short enough to prevent deadlocks if a holder crashes mid-operation.
func NewProductLock(client *redisShared.ClientWrapper, productID string, logger *zap.Logger) *RedlockWrapper {
	return &RedlockWrapper{
		lock: redisShared.NewRedlock(client, lockKey(productID), 30*time.Second, 5),
		// Re-acquire up to 5 times before giving up.
		productID: productID,
		logger:    logger,
		tracer:    otel.Tracer("wire-inventory/locking"),
	}
}

// Acquire attempts to grab the distributed lock for this product with a deadline.
// Returns ErrLockTimeout if the lock cannot be acquired within `timeout`.
func (w *RedlockWrapper) Acquire(ctx context.Context, timeout time.Duration) error {
	ctx, span := w.tracer.Start(ctx, "RedlockWrapper.Acquire")
	defer span.End()

	span.SetAttributes(
		attribute.String("product.id", w.productID),
		attribute.String("lock.key", lockKey(w.productID)),
		attribute.String("timeout", timeout.String()),
	)

	// Wrap context with deadline so we never wait forever.
	acquireCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := w.lock.Acquire(acquireCtx, w.logger)
	if err != nil {
		if errors.Is(err, redisShared.ErrLockAlreadyHeld) {
			return &ErrLockTimeout{ProductID: w.productID, Timeout: timeout}
		}
		return fmt.Errorf("acquire redlock for product %s: %w", w.productID, err)
	}

	w.owned = true
	w.logger.Info("distributed lock acquired",
		zap.String("product_id", w.productID),
		zap.Duration("ttl", 30*time.Second),
	)
	return nil
}

// Release releases the distributed lock. Safe to call even if we don't own it —
// the underlying Redlock uses a token-based delete to prevent releasing another holder's lock.
func (w *RedlockWrapper) Release() error {
	if !w.owned {
		return nil // Nothing to release.
	}

	ctx, span := w.tracer.Start(context.Background(), "RedlockWrapper.Release")
	defer span.End()

	span.SetAttributes(attribute.String("product.id", w.productID))

	err := w.lock.Release(ctx, w.logger)
	if err != nil && errors.Is(err, redisShared.ErrReleaseMismatch) {
		w.logger.Warn("released lock we no longer owned (expected after timeout)",
			zap.String("product_id", w.productID),
		)
		// Non-fatal: another instance took over after our TTL expired.
		w.owned = false
		return nil
	}
	if err != nil {
		return fmt.Errorf("release redlock for product %s: %w", w.productID, err)
	}

	w.owned = false
	w.logger.Debug("distributed lock released", zap.String("product_id", w.productID))
	return nil
}

// HasOwnership returns true if we successfully acquired this lock and have not
// yet released it. This is an in-memory check that is sufficient for normal use —
// a Redis key may expire, but that is handled by the Release mismatch path above.
func (w *RedlockWrapper) HasOwnership() bool {
	return w.owned
}

// =============================================================================
// Error types
// =============================================================================

// ErrLockTimeout indicates the distributed lock could not be acquired within the deadline.
type ErrLockTimeout struct {
	ProductID string
	Timeout   time.Duration
}

func (e *ErrLockTimeout) Error() string {
	return fmt.Sprintf("redlock timeout for product %s after %s", e.ProductID, e.Timeout)
}


