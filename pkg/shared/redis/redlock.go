package redis

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

var (
	// ErrLockAlreadyHeld is returned when a lock is already held by another holder.
	ErrLockAlreadyHeld = errors.New("redlock: lock already acquired by another holder")

	// ErrReleaseMismatch is returned when the lock token doesn't match current value.
	ErrReleaseMismatch = errors.New("redlock: release mismatch - cannot release unowned lock")
)

const redlockScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
`

// Redlock implements a distributed lock using Redis with token-based lease verification.
type Redlock struct {
	client     *ClientWrapper
	key        string
	lockValue  []byte // unique token identifying this lock owner
	ttl        time.Duration
	retryCount int
}

// NewRedlock creates a new distributed lock instance.
func NewRedlock(client *ClientWrapper, key string, ttl time.Duration, retryCount int) *Redlock {
	return &Redlock{
		client:     client,
		key:        key,
		ttl:        ttl,
		retryCount: retryCount,
	}
}

// Acquire attempts to acquire a distributed lock with the given timeout.
func (r *Redlock) Acquire(ctx context.Context, logger *zap.Logger) error {
	r.lockValue = []byte(fmt.Sprintf("%x", rand.New(rand.NewSource(time.Now().UnixNano())).Int63()))

	attempts := r.retryCount + 1
	for i := 0; i < attempts; i++ {
		err := r.tryAcquire(ctx, logger)
		if err == nil {
			return nil
		}

		if i < attempts-1 {
			backoff := time.Duration(i) * time.Millisecond * time.Duration(i+1) * 2
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			logger.Debug("redlock retry", zap.Int("attempt", i+1), zap.Duration("backoff", backoff+jitter))
			time.Sleep(backoff + jitter)
		}
	}

	return ErrLockAlreadyHeld
}

// tryAcquire attempts to set the lock key if it doesn't exist.
func (r *Redlock) tryAcquire(ctx context.Context, logger *zap.Logger) error {
	okCmd := r.client.Client.SetNX(ctx, r.key, string(r.lockValue), r.ttl)
	ok, err := okCmd.Result()
	if err != nil {
		return fmt.Errorf("redlock setnx: %w", err)
	}

	if !ok {
		logger.Debug("redlock not acquired (already held)", zap.String("key", r.key))
		return ErrLockAlreadyHeld
	}

	logger.Info("redlock acquired", zap.String("key", r.key), zap.Duration("ttl", r.ttl))
	return nil
}

// Release releases the distributed lock. Returns error if the caller doesn't own it.
func (r *Redlock) Release(ctx context.Context, logger *zap.Logger) error {
	result, err := r.client.Client.Eval(ctx, redlockScript, []string{r.key}, string(r.lockValue)).Int64()
	if err != nil {
		return fmt.Errorf("redlock release: %w", err)
	}

	if result == 0 {
		return ErrReleaseMismatch
	}

	logger.Info("redlock released", zap.String("key", r.key))
	return nil
}

// IsHeld returns true if the lock is currently held by any holder (including this one).
func (r *Redlock) IsHeld(ctx context.Context, logger *zap.Logger) (bool, error) {
	val, err := r.client.Client.Get(ctx, r.key).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return false, fmt.Errorf("redlock isheld: %w", err)
	}

	return val != "", nil
}

// Extend refreshes the lock TTL without releasing it.
func (r *Redlock) Extend(ctx context.Context, logger *zap.Logger, newTTL time.Duration) error {
	result, err := r.client.Client.Eval(ctx, redlockScript, []string{r.key}, string(r.lockValue)).Int64()
	if err != nil {
		return fmt.Errorf("redlock extend: %w", err)
	}

	if result == 0 {
		return ErrReleaseMismatch
	}

	okCmd := r.client.Client.SetNX(ctx, r.key, string(r.lockValue), newTTL)
	ok, err := okCmd.Result()
	if err != nil {
		return fmt.Errorf("redlock re-set after extend: %w", err)
	}

	_ = ok
	logger.Debug("redlock extended", zap.String("key", r.key), zap.Duration("new_ttl", newTTL))
	return nil
}
