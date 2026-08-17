// Package middleware provides shared HTTP middleware for WPO services including
// request ID generation, structured logging, and OpenTelemetry tracing propagation.
// Thomas Wire <tom@thomaswire.com>
package middleware

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

// RequestID generates a unique request ID if not already present.
func RequestID(logger *zap.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqID := r.Header.Get("X-Request-ID")
			if reqID == "" {
				reqID = uuid.New().String()
			}

			w.Header().Set("X-Request-ID", reqID)
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, reqID)))
		})
	}
}

// Logger logs incoming requests with status, duration, and trace correlation.
func Logger(logger *zap.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Wrap ResponseWriter to capture status code
			wrapped := &statusWriter{ResponseWriter: w, statusCode: http.StatusOK}

			next.ServeHTTP(wrapped, r)

			requestID := getRequestID(r.Context())
			duration := time.Since(start)

			logger.Info("http request",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", wrapped.statusCode),
				zap.Duration("duration_ms", duration),
				zap.String("request_id", requestID),
				zap.String("user_agent", r.UserAgent()),
			)
		})
	}
}

// Recovery recovers from panics and returns a 500 Internal Server Error.
func Recovery(logger *zap.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					logger.Error("recovered from panic", zap.Any("error", err))
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"error":"internal_server_error"}`))
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}

// Tracing propagates W3C trace context through incoming requests.
func Tracing() func(next http.Handler) http.Handler {
	propagator := propagation.TraceContext{}
	return func(next http.Handler) http.Handler {
		return otelhttp.NewHandler(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
				r = r.WithContext(ctx)
				next.ServeHTTP(w, r)
			}),
			"", // name from inner handler
		)
	}
}

// statusWriter wraps http.ResponseWriter to capture the status code.
type statusWriter struct {
	http.ResponseWriter
	statusCode int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.statusCode = code
	sw.ResponseWriter.WriteHeader(code)
}

type requestIDKey struct{}

func getRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey{}).(string); ok {
		return id
	}
	return "unknown"
}
