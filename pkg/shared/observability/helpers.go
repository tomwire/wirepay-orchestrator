package observability

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"os"

	"go.uber.org/zap"
)

// HTTPError writes a structured JSON error response with the given status code.
func HTTPError(w http.ResponseWriter, logger *zap.Logger, statusCode int, err error, msg string) {
	logger.Error(msg, zap.Int("status", statusCode), zap.Error(err))
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"error":"` + truncate(msg, 200) + `"}`))
}

// HTTPSuccess writes a JSON success response.
func HTTPSuccess(w http.ResponseWriter, status int, data interface{}, logger *zap.Logger) {
	logger.Debug("success response", zap.Int("status", status), zap.Any("data", data))
	w.Header().Set("Content-Type", "application/json")
}

// GenerateRequestID returns a random 16-byte hex string suitable for request IDs.
func GenerateRequestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "...[truncated]"
	}
	return s
}

// getEnvOrDefault returns an environment variable or a fallback.
// Uses os.Getenv from the standard library.
func getEnvOrDefault(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}
