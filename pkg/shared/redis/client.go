// Package redis provides shared Redis client initialization and helper wrappers.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

// ClientWrapper provides additional helper methods on top of go-redis.
type ClientWrapper struct {
	*goredis.Client
}

// NewClient creates a new Redis client with the given configuration.
func NewClient(logger *zap.Logger, url string, timeout time.Duration, poolSize int) (*goredis.Client, error) {
	opts := &goredis.Options{
		Addr:      url,
		Password:  "",
		PoolSize:  poolSize,
		DialTimeout: timeout,
	}

	client := goredis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		logger.Error("redis ping failed", zap.Error(err), zap.String("url", url))
		return nil, fmt.Errorf("redis connection: %w", err)
	}

	logger.Info("redis connected successfully")
	return client, nil
}

// NewClientWrapper wraps an existing Redis client with helper methods.
func NewClientWrapper(client *goredis.Client) *ClientWrapper {
	return &ClientWrapper{Client: client}
}

// Close shuts down the Redis connection pool.
func (c *ClientWrapper) Close() error {
	if c == nil || c.Client == nil {
		return nil
	}
	return c.Client.Close()
}

// Set stores a key-value pair in Redis with an optional expiration.
func (c *ClientWrapper) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal redis value: %w", err)
	}

	_, span := otel.Tracer("wirepay/redis").Start(ctx, "redis.set")
	defer span.End()

	cmd := c.Client.Set(ctx, key, data, expiration)
	if err := cmd.Err(); err != nil {
		return fmt.Errorf("redis set %s: %w", key, err)
	}

	return nil
}

// Get retrieves a value from Redis by key and unmarshals into dest.
func (c *ClientWrapper) Get(ctx context.Context, key string, dest interface{}) error {
	cmd := c.Client.Get(ctx, key)
	if err := cmd.Err(); err != nil {
		return fmt.Errorf("redis get %s: %w", key, err)
	}

	data, err := cmd.Bytes()
	if err != nil {
		return fmt.Errorf("redis get bytes: %w", err)
	}

	if len(data) == 0 {
		return goredis.Nil
	}

	err = json.Unmarshal(data, dest)
	if err != nil {
		return fmt.Errorf("unmarshal redis value for %s: %w", key, err)
	}

	return nil
}
