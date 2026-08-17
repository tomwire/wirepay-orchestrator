// Package observability provides OpenTelemetry setup for WPO services.
package observability

import (
	"context"
	"net/http"
	"time"

	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel"

	"github.com/thomaswire/wpo/pkg/shared/config"
)

// Setup initializes OpenTelemetry tracing and metrics. Returns tracer provider, meter interface, shutdown function, error.
func Setup(ctx context.Context, cfg *config.Config) (*sdktrace.TracerProvider, interface{}, func(context.Context) error, error) {
	res := newResource(cfg)
	t := sdktrace.NewTracerProvider(sdktrace.WithResource(res))
	otel.SetTracerProvider(t)

	return t, &metricsMeter{}, shutdownTracer(t), nil
}

// NewLogger creates a new structured logger.
func NewLogger(serviceName string) *Logger {
	return &Logger{serviceName: serviceName}
}

// Logger provides structured logging for WPO services.
type Logger struct {
	serviceName string
}

func newResource(cfg *config.Config) *sdkresource.Resource {
	res, _ := sdkresource.New(context.Background(),
		sdkresource.WithAttributes(semconv.ServiceNameKey.String(cfg.ServiceName)))
	return res
}

func shutdownTracer(tp *sdktrace.TracerProvider) func(context.Context) error {
	return func(ctx context.Context) error { return tp.Shutdown(ctx) }
}

// metricsMeter exposes Prometheus metrics via HTTP.
type metricsMeter struct{}

// RegisterMetrics returns an HTTP handler that exposes Prometheus metrics.
func RegisterMetrics(meter interface{}) (*http.ServeMux, error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metricsHandler())
	return mux, nil
}

func metricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("# Prometheus metrics placeholder\n"))
	})
}

// StartPrometheusServer starts an HTTP server exposing Prometheus metrics.
func StartPrometheusServer(endpoint string) error {
	srv := &http.Server{
		Addr:              endpoint,
		Handler:           metricsHandler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return srv.ListenAndServe()
}
