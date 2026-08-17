// Package main provides the entrypoint for wire-payment service.
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

	"github.com/thomaswire/wpo/cmd/wire-payment/handlers"
	paymentRepo "github.com/thomaswire/wpo/cmd/wire-payment/repository"
	"github.com/thomaswire/wpo/cmd/wire-payment/idempotency"
	"github.com/thomaswire/wpo/cmd/wire-payment/service"
	"github.com/thomaswire/wpo/pkg/shared/config"
	"github.com/thomaswire/wpo/pkg/shared/observability"
	"github.com/thomaswire/wpo/pkg/shared/postgres"
	redisShared "github.com/thomaswire/wpo/pkg/shared/redis"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load("wire-payment")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db, err := postgres.NewPool(ctx, cfg)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer db.Close()

	redisClient, err := redisShared.NewClient(logger, cfg.RedisURL, cfg.RedisDialTimeout, cfg.RedisPoolSize)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer redisClient.Close()

	tracer, _, shutdownOtel, err := observability.Setup(ctx, cfg)
	if err != nil {
		log.Fatalf("otel: %v", err)
	}
	defer func() { _ = shutdownOtel(context.Background()) }()

	_ = tracer

	repo := paymentRepo.NewPaymentRepo(db, logger)

	// Create Redis token storage for idempotency
	redisWrapper := redisShared.NewClientWrapper(redisClient)
	tokens := idempotency.NewTokenStorage(redisWrapper.Client, 24*time.Hour, logger)

	// Use a no-op payment provider for now (real Stripe integration would go here)
	svc := service.NewPaymentService(repo, nil, tokens, logger, "wire-payment")

	r := mux.NewRouter()
	r.HandleFunc("/health", healthHandler).Methods("GET")
	r.HandleFunc("/ready", readyHandler).Methods("GET")
	handlers.RegisterRoutes(r, svc, logger)

	srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.ServicePort), Handler: r}
	go srv.ListenAndServe()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	cancel()
	srv.Shutdown(context.Background())
	logger.Info("wire-payment stopped")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok","service":"wire-payment"}`))
}

func readyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ready","service":"wire-payment"}`))
}
