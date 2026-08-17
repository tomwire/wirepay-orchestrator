// Package config provides a centralized configuration loader for WPO services.
// Thomas Wire <tom@thomaswire.com>
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all configuration for a WPO service, loaded from environment variables
// with sensible defaults. Each service instantiates its own copy.
type Config struct {
	ServiceName string
	ServicePort int

	// PostgreSQL
	PostgresDSN        string
	PostgresMaxConns   int
	PostgresMinConns   int
	PostgresConnectTimeout time.Duration

	// Redis
	RedisURL         string
	RedisDialTimeout time.Duration
	RedisPoolSize    int

	// Kafka
	KafkaBootstrapServers  string
	KafkaConsumerGroup     string
	KafkaTopicPrefix       string
	KafkaProducerAckMode   string // "all", "1", "0"
	KafkaMaxReconnectWaitMs int
	KafkaSessionTimeoutMs  int

	// Observability
	OtelExporterEndpoint string
	OtelServiceName      string
	OtelSamplerRatio     float64

	// Circuit Breaker
	CircuitBreakerMaxFailures  int
	CircuitBreakerResetTimeout time.Duration

	// Idempotency (Redis TTL for tokens)
	IdempotencyTokenTTL time.Duration

	// Stripe mock mode flag
	StripeMockMode bool
}

// Load creates a Config from environment with defaults.
// Override individual fields by setting environment variables before calling.
func Load(serviceName string) *Config {
	return &Config{
		ServiceName: serviceName,

		ServicePort: envInt("SERVICE_PORT", 8081),

		PostgresDSN:        envOrDefault("DATABASE_URL", "postgres://wpo:wpo@localhost:5432/wpo"),
		PostgresMaxConns:   envInt("PG_MAX_CONNS", 20),
		PostgresMinConns:   envInt("PG_MIN_CONNS", 2),
		PostgresConnectTimeout: 10 * time.Second,

		RedisURL:         envOrDefault("REDIS_URL", "redis://localhost:6379"),
		RedisDialTimeout: 5 * time.Second,
		RedisPoolSize:    envInt("REDIS_POOL_SIZE", 5),

		KafkaBootstrapServers:   envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		KafkaConsumerGroup:      fmt.Sprintf("%s-group", serviceName),
		KafkaTopicPrefix:        envOrDefault("KAFKA_TOPIC_PREFIX", serviceName+"-"),
		KafkaProducerAckMode:    envOrDefault("KAFKA_PRODUCER_ACKS", "all"),
		KafkaMaxReconnectWaitMs: envInt("KAFKA_MAX_RECONNECT_WAIT", 2000),
		KafkaSessionTimeoutMs:   envInt("KAFKA_SESSION_TIMEOUT", 30000),

		OtelExporterEndpoint: envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
		OtelServiceName:      serviceName,
		OtelSamplerRatio:     float64(envInt("OTEL_SAMPLER_RATIO", 10)),

		CircuitBreakerMaxFailures:  envInt("CIRCUIT_BREAKER_MAX_FAILURES", 5),
		CircuitBreakerResetTimeout: 30 * time.Second,

		IdempotencyTokenTTL: 24 * time.Hour,

		StripeMockMode: envBool("STRIPE_MOCK_MODE", true),
	}
}

// DSN returns the constructed PostgreSQL connection string for a named database.
func (c *Config) DSN(dbName string) string {
	if c.PostgresDSN == "" {
		return fmt.Sprintf("postgres://wpo:wpo@localhost:5432/%s", dbName)
	}
	// Replace the default dbname in existing URL if present
	return replaceLastPathSegment(c.PostgresDSN, dbName)
}

func replaceLastPathSegment(dsn, replacement string) string {
	idx := -1
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			idx = i
			break
		}
	}
	if idx < 0 {
		return dsn + "/" + replacement
	}
	return dsn[:idx+1] + replacement
}

// Helper functions for environment variable reading with defaults

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envInt(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return n
}

func envBool(key string, defaultVal bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return defaultVal
	}
	return b
}
