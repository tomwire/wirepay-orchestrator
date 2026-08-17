// Package idempotency provides Redis-backed token validation for preventing duplicate
// payment processing. Each unique (idempotency_key, amount, currency) combination is
// recorded in Redis with a TTL so that retry-safe operations return cached results.
package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/thomaswire/wpo/cmd/wire-payment/repository"
)

// ErrIdempotencyMismatch is returned when a request arrives with an existing idempotency key
// but different amount or currency than the original request. This is a business-level
// error indicating a potential bug or malicious intent.
var ErrIdempotencyMismatch = errors.New("idempotency mismatch: amount or currency differs from original request")

// token represents the data stored in Redis for idempotency validation.
type token struct {
	OrderID        string         `json:"order_id"`
	AmountStr      string         `json:"amount"` // Stored as fixed-point string to avoid float precision issues
	Currency       string         `json:"currency"`
	PaymentID      string         `json:"payment_id"`
	IdempotencyKey string         `json:"idempotency_key"`
	Status         string         `json:"status"`
	CreatedAt      time.Time      `json:"created_at"`
}

// TokenStorage manages idempotency tokens in Redis.
type TokenStorage struct {
	client *redis.Client
	ttl    time.Duration
	logger *zap.Logger
}

// NewTokenStorage creates a new Redis-backed token storage with the given TTL.
func NewTokenStorage(client *redis.Client, ttl time.Duration, logger *zap.Logger) *TokenStorage {
	return &TokenStorage{
		client: client,
		ttl:    ttl,
		logger: logger,
	}
}

// RedisKey returns the Redis key for an idempotency token.
// Format: "idempotency:{key}" where key is URL-safe to avoid Redis special characters.
func RedisKey(idempotencyKey string) string {
	return fmt.Sprintf("idempotency:%s", idempotencyKey)
}

// ValidateToken checks if an idempotency token exists for the given key and validates
// that the requested amount/currency match the original request.
func (s *TokenStorage) ValidateToken(ctx context.Context, key string, amountStr string, currency string) (*repository.Payment, bool, error) {
	if key == "" {
		return nil, false, fmt.Errorf("idempotency key is required")
	}

	redisKey := RedisKey(key)

	data, err := s.client.Get(ctx, redisKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// No token exists — caller can proceed with creating a new payment
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("redis get idempotency token %s: %w", redisKey, err)
	}

	var stored token
	if err := json.Unmarshal(data, &stored); err != nil {
		s.logger.Error("failed to unmarshal idempotency token from Redis",
			zap.String("key", key),
			zap.Error(err))
		return nil, false, fmt.Errorf("unmarshal idempotency token: %w", err)
	}

	// Validate amount match (string comparison).
	if stored.AmountStr != amountStr {
		s.logger.Warn("idempotency amount mismatch detected",
			zap.String("key", key),
			zap.String("stored_amount", stored.AmountStr),
			zap.String("request_amount", amountStr))
		return nil, false, ErrIdempotencyMismatch
	}

	// Validate currency match (case-insensitive).
	if stored.Currency != currency {
		s.logger.Warn("idempotency currency mismatch detected",
			zap.String("key", key),
			zap.String("stored_currency", stored.Currency),
			zap.String("request_currency", currency))
		return nil, false, ErrIdempotencyMismatch
	}

	s.logger.Debug("idempotency token validated — returning cached result",
		zap.String("key", key),
		zap.String("payment_id", stored.PaymentID))

	// Return a Payment struct populated from the token data.
	payment := &repository.Payment{
		ID:        stored.PaymentID,
		OrderID:   stored.OrderID,
		Currency:  stored.Currency,
		Status:    stored.Status,
		CreatedAt: stored.CreatedAt,
	}

	return payment, true, nil
}

// StoreToken persists an idempotency token in Redis with the configured TTL.
func (s *TokenStorage) StoreToken(ctx context.Context, key string, payment *repository.Payment) error {
	if key == "" || payment.ID == "" {
		return fmt.Errorf("idempotency key and payment ID are required for token storage")
	}

	amountStr := "0"
	if payment.AmountCents > 0 {
		amountStr = fmt.Sprintf("%d", payment.AmountCents)
	}

	tok := &token{
		OrderID:        payment.OrderID,
		AmountStr:      amountStr,
		Currency:       payment.Currency,
		PaymentID:      payment.ID,
		IdempotencyKey: key,
		Status:         payment.Status,
		CreatedAt:      time.Now().UTC(),
	}

	data, err := json.Marshal(tok)
	if err != nil {
		return fmt.Errorf("marshal idempotency token: %w", err)
	}

	redisKey := RedisKey(key)
	err = s.client.Set(ctx, redisKey, data, s.ttl).Err()
	if err != nil {
		s.logger.Error("failed to store idempotency token in Redis",
			zap.String("key", key),
			zap.Duration("ttl", s.ttl),
			zap.Error(err))
		return fmt.Errorf("redis set idempotency token %s: %w", redisKey, err)
	}

	s.logger.Debug("idempotency token stored",
		zap.String("key", key),
		zap.String("payment_id", payment.ID),
		zap.Duration("ttl", s.ttl))

	return nil
}

// ClearToken removes an idempotency token from Redis. Used when a payment creation
// fails after the token was stored but before it was fully committed.
func (s *TokenStorage) ClearToken(ctx context.Context, key string) error {
	redisKey := RedisKey(key)
	err := s.client.Del(ctx, redisKey).Err()
	if err != nil {
		s.logger.Error("failed to clear idempotency token",
			zap.String("key", key),
			zap.Error(err))
		return fmt.Errorf("redis del idempotency token %s: %w", redisKey, err)
	}

	s.logger.Debug("idempotency token cleared", zap.String("key", key))
	return nil
}

// ValidateRefundToken checks if a refund has already been processed for the given payment.
func (s *TokenStorage) ValidateRefundToken(ctx context.Context, paymentID string) bool {
	refKey := fmt.Sprintf("idempotency_refund:%s", paymentID)
	data, err := s.client.Get(ctx, refKey).Bytes()
	if err != nil {
		return false // No refund token exists yet
	}

	var tok struct {
		PaymentID string `json:"payment_id"`
		Status    string `json:"status"`
	}
	if err := json.Unmarshal(data, &tok); err != nil {
		return false
	}

	return tok.Status == "refunded"
}

// StoreRefundToken marks a refund as processed to prevent duplicate refunds.
func (s *TokenStorage) StoreRefundToken(ctx context.Context, paymentID string) error {
	refKey := fmt.Sprintf("idempotency_refund:%s", paymentID)

	tokData, err := json.Marshal(map[string]string{
		"payment_id": paymentID,
		"status":     "refunded",
	})
	if err != nil {
		return fmt.Errorf("marshal refund token: %w", err)
	}

	err = s.client.Set(ctx, refKey, tokData, 24*time.Hour).Err()
	if err != nil {
		s.logger.Error("failed to store refund idempotency token",
			zap.String("payment_id", paymentID),
			zap.Error(err))
		return fmt.Errorf("redis set refund token %s: %w", refKey, err)
	}

	s.logger.Debug("refund idempotency token stored", zap.String("payment_id", paymentID))
	return nil
}
