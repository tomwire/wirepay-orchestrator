// Package httpserver provides a shared HTTP server setup with graceful shutdown,
// health/readiness endpoints, and common middleware wiring for WPO services.
// Thomas Wire <tom@thomaswire.com>
package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/thomaswire/wpo/pkg/shared/config"
	"github.com/thomaswire/wpo/pkg/shared/middleware"
)

// Server wraps an HTTP server with health/readiness handlers and graceful shutdown.
type Server struct {
	router *mux.Router
	server *http.Server
	logger *zap.Logger
}

// New creates a new WPO service HTTP server with standard middleware and health endpoints.
func New(cfg *config.Config, logger *zap.Logger, routes func(r *mux.Router)) *Server {
	r := mux.NewRouter()

	// Standard middleware chain: request ID → tracing → logging → recovery
	r.Use(middleware.RequestID(logger))
	r.Use(middleware.Tracing())
	r.Use(middleware.Logger(logger))
	r.Use(middleware.Recovery(logger))

	// Health and readiness endpoints
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","service":"` + cfg.ServiceName + `"}`))
	}).Methods("GET")

	r.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		// Ready check can verify dependent services (DB, Redis, Kafka) are up.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready","service":"` + cfg.ServiceName + `"}`))
	}).Methods("GET")

	// Register service-specific routes
	routes(r)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.ServicePort),
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return &Server{
		router: r,
		server: srv,
		logger: logger,
	}
}

// Start begins listening for HTTP requests. Blocks until the server is closed.
func (s *Server) Start() error {
	s.logger.Info("starting HTTP server", zap.String("addr", s.server.Addr))

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	return nil
}

// Shutdown gracefully shuts down the HTTP server with a timeout.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down HTTP server")

	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	return s.server.Shutdown(shutdownCtx)
}

// Port returns the configured service port number.
func (s *Server) Port() int {
	parts := strings.Split(s.server.Addr, ":")
	if len(parts) == 2 {
		port, _ := strconv.Atoi(parts[1])
		return port
	}
	return 8081
}

// Addr returns the full bind address.
func (s *Server) Addr() string {
	return s.server.Addr
}

// ListenAndServe starts the server (simplified wrapper).
func ListenAndServe(addr string, handler http.Handler, logger *zap.Logger) error {
	logger.Info("HTTP server starting", zap.String("addr", addr))
	return http.ListenAndServe(addr, handler)
}
