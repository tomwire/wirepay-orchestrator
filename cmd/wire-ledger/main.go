// Package main provides the entrypoint for wire-ledger service — double-entry
// bookkeeping with atomic transaction posting and automatic balance updates.
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

	ledgerHandlers "github.com/thomaswire/wpo/cmd/wire-ledger/handlers"
	"github.com/thomaswire/wpo/cmd/wire-ledger/repository"
	"github.com/thomaswire/wpo/cmd/wire-ledger/service"
	"github.com/thomaswire/wpo/pkg/shared/config"
	kafkaShared "github.com/thomaswire/wpo/pkg/shared/kafka"
	"github.com/thomaswire/wpo/pkg/shared/observability"
	"github.com/thomaswire/wpo/pkg/shared/postgres"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load("wire-ledger")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db, err := postgres.NewPool(ctx, cfg)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer db.Close()

	tracer, _, shutdownOtel, err := observability.Setup(ctx, cfg)
	if err != nil {
		log.Fatalf("otel: %v", err)
	}
	defer func() { _ = shutdownOtel(context.Background()) }()

	_ = tracer

	// Kafka is optional for ledger (events are mostly internal), but we set it up
	// in case we want to publish balance events or reconciliation reports.
	kafkaClient, kafkaErr := kafkaShared.NewSyncProducer(logger, cfg.KafkaBootstrapServers, "ledger-events")
	if kafkaErr != nil {
		log.Printf("kafka: %v (continuing without)", kafkaErr)
	} else {
		defer func() { _ = kafkaClient.Close() }()
	}

	_ = kafkaClient

	repo := repository.NewLedgerRepo(db, logger)
	svc := service.NewLedgerService(repo, logger, nil)

	r := mux.NewRouter()
	r.HandleFunc("/health", healthHandler).Methods("GET")
	r.HandleFunc("/ready", readyHandler).Methods("GET")
	ledgerHandlers.RegisterRoutes(r, svc, logger)

	srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.ServicePort), Handler: r}
	go srv.ListenAndServe()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	cancel()
	srv.Shutdown(context.Background())
	logger.Info("wire-ledger stopped")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok","service":"wire-ledger"}`))
}

func readyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ready","service":"wire-ledger"}`))
}
