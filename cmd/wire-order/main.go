// Package main is the entry point for the wire-order service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	gopostgres "github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/thomaswire/wpo/cmd/wire-order/handlers"
	"github.com/thomaswire/wpo/cmd/wire-order/repository"
	"github.com/thomaswire/wpo/cmd/wire-order/service"
	pgxsharedpostgres "github.com/thomaswire/wpo/pkg/shared/postgres"
	redisshared "github.com/thomaswire/wpo/pkg/shared/redis"
	wpoobservability "github.com/thomaswire/wpo/pkg/shared/observability"
	wpokat "github.com/thomaswire/wpo/pkg/shared/kafka"
	"github.com/thomaswire/wpo/pkg/shared/config"
)

var serviceVersion = "0.1.0-dev"

func main() {
	if err := run(); err != nil {
		log.Fatalf("wire-order: fatal error: %v", err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Load("wire-order")

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	logger.Info("starting wire-order service",
		zap.String("version", serviceVersion),
		zap.Int("port", cfg.ServicePort))

	// Initialize PostgreSQL pool
	pool, err := pgxsharedpostgres.NewPool(ctx, cfg)
	if err != nil {
		return fmt.Errorf("wire-order: postgres connection failed: %w", err)
	}
	defer pool.Close()

	logger.Info("connected to PostgreSQL")

	// Run migrations
	migrationsDir, err := findMigrationsDir()
	if err != nil {
		return fmt.Errorf("wire-order: locate migrations dir: %w", err)
	}

	if err := pgxsharedpostgres.Migrate(ctx, logger, pool, migrationsDir); err != nil {
		return fmt.Errorf("wire-order: migration failed: %w", err)
	}
	logger.Info("migrations applied successfully")

	// Initialize Redis client
	redisRaw, err := redisshared.NewClient(logger, cfg.RedisURL, cfg.RedisDialTimeout, cfg.RedisPoolSize)
	if err != nil {
		return fmt.Errorf("wire-order: redis connection failed: %w", err)
	}
	redisWrapper := redisshared.NewClientWrapper(redisRaw)
	defer func() { _ = redisWrapper.Close() }()

	logger.Info("connected to Redis")

	// Set up OpenTelemetry tracing
	tracer, _, shutdownObservability, err := wpoobservability.Setup(ctx, cfg)
	if err != nil {
		logger.Warn("observability setup failed (non-fatal)", zap.Error(err))
	} else {
		otel.SetTracerProvider(tracer)
		defer func() { _ = shutdownObservability(ctx) }()
		logger.Info("OpenTelemetry initialized")
	}

	// Set up Kafka producer + consumer
	var kafkaProducer *wpokat.Producer
	kafkaProducer, err = wpokat.NewSyncProducer(logger, cfg.KafkaBootstrapServers)
	if err != nil {
		logger.Warn("kafka producer init failed (non-fatal), outbox publishing disabled", zap.Error(err))
	}

	orderRepo := repository.NewPostgresOrderRepo(pool)
	outboxPub := &simpleOutboxPublisher{pool: pool, kafkaProducer: kafkaProducer, serviceName: cfg.ServiceName}
	svc := service.NewOrderService(orderRepo, logger, outboxPub, nil, nil)

	r := mux.NewRouter()
	r.HandleFunc("/health", healthHandler).Methods("GET")
	r.HandleFunc("/ready", readyHandler).Methods("GET")
	handlers.RegisterRoutes(r, &handlers.Dependencies{
		Service: svc,
		Repo:    orderRepo,
		Logger:  logger,
	})

	srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.ServicePort), Handler: r}
	go srv.ListenAndServe()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	cancel()
	srv.Shutdown(context.Background())
	logger.Info("wire-order stopped")

	return nil
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok","service":"wire-order"}`))
}

func readyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ready","service":"wire-order"}`))
}

// simpleOutboxPublisher implements the OrderPublisher interface with outbox pattern.
type simpleOutboxPublisher struct {
	pool          *gopostgres.Pool
	kafkaProducer *wpokat.Producer
	serviceName   string
}

func (p *simpleOutboxPublisher) WriteToOutbox(ctx context.Context, eventType string, key string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}

	const sql = `INSERT INTO outbox_events (id, event_type, service_name, "key", payload, status) VALUES ($1, $2, $3, $4, $5, 'pending')`
	id := fmt.Sprintf("outbox-%d", time.Now().UnixNano())
	_, err = p.pool.Exec(ctx, sql, id, eventType, p.serviceName, key, data)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	_ = data
	return nil
}

func findMigrationsDir() (string, error) {
	dir := "cmd/wire-order/migrations"
	if info, err := os.Stat(dir); err == nil && info.IsDir() {
		return dir, nil
	}
	return "", fmt.Errorf("migrations directory not found: %s", dir)
}
