// Package main provides the entrypoint for wire-inventory service.
// It manages stock reservations with atomic locking via Redis Redlock + DB-level
// SELECT FOR UPDATE to prevent overselling during flash-sale scenarios.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	inventoryRepo "github.com/thomaswire/wpo/cmd/wire-inventory/repository"
	"github.com/thomaswire/wpo/cmd/wire-inventory/service"
	"github.com/thomaswire/wpo/pkg/shared/config"
	kafkaShared "github.com/thomaswire/wpo/pkg/shared/kafka"
	redisShared "github.com/thomaswire/wpo/pkg/shared/redis"

	pgxsharedpostgres "github.com/thomaswire/wpo/pkg/shared/postgres"
)

// InventoryService wraps inventory logic for HTTP routing.
type InventoryService struct {
	svc    *service.InventoryService
	logger *zap.Logger
}

// NewInventoryService constructs the service and HTTP layer.
func NewInventoryService(svc *service.InventoryService, logger *zap.Logger) *InventoryService {
	return &InventoryService{svc: svc, logger: logger}
}

// healthHandler handles GET /health
func (h *InventoryService) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok","service":"wire-inventory"}`))
}

// readyHandler handles GET /ready
func (h *InventoryService) readyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ready","service":"wire-inventory"}`))
}

// main bootstraps and starts the wire-inventory service.
func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load("wire-inventory")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxsharedpostgres.NewPool(ctx, cfg)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer pool.Close()

	redisClient, redisErr := redisShared.NewClient(logger, cfg.RedisURL, cfg.RedisDialTimeout, cfg.RedisPoolSize)
	if redisErr != nil {
		logger.Warn("redis: continuing without (stock locking disabled)", zap.Error(redisErr))
	}
	defer func() {
		if redisClient != nil {
			redisClient.Close()
		}
	}()

	kafkaProducer, kafkaErr := kafkaShared.NewSyncProducer(logger, cfg.KafkaBootstrapServers)
	if kafkaErr != nil {
		logger.Warn("kafka: continuing without producer", zap.Error(kafkaErr))
	}

	repo := inventoryRepo.NewInventoryRepo(pool, logger)

	var wrapper *redisShared.ClientWrapper
	if redisClient != nil {
		wrapper = redisShared.NewClientWrapper(redisClient)
	}

	svc := service.NewInventoryService(repo, logger, wrapper, kafkaProducer)
	wrapSrv := NewInventoryService(svc, logger)

	r := mux.NewRouter()
	r.HandleFunc("/health", wrapSrv.healthHandler).Methods("GET")
	r.HandleFunc("/ready", wrapSrv.readyHandler).Methods("GET")

	srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.ServicePort), Handler: r}
	go srv.ListenAndServe()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	cancel()
	srv.Shutdown(context.Background())
	logger.Info("wire-inventory stopped")
}
