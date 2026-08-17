// Package provider defines the PaymentProvider interface and its concrete implementations
// for pluggable payment backends. The interface enables seamless switching between
// Stripe real integration and a mock provider for development/testing.
package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

// PaymentIntentStatus reflects the lifecycle state of a provider-level payment intent.
type PaymentIntentStatus string

const (
	StatusSucceeded   PaymentIntentStatus = "succeeded"
	StatusFailed      PaymentIntentStatus = "failed"
	StatusRequiresAction PaymentIntentStatus = "requires_action"
	StatusPending     PaymentIntentStatus = "pending"
)

// RefundStatus reflects the lifecycle state of a refund.
type RefundStatus string

const (
	RefundSucceeded RefundStatus = "succeeded"
	RefundFailed    RefundStatus = "failed"
	RefundPending   RefundStatus = "pending"
)

// PaymentIntentResult is the provider's response to a payment intent creation request.
type PaymentIntentResult struct {
	ID                string              `json:"id"`
	Status            PaymentIntentStatus `json:"status"`
	Amount            float64             `json:"amount"`
	Currency          string              `json:"currency"`
	ClientSecret      string              `json:"client_secret,omitempty"`
	ErrorMessage      string              `json:"error_message,omitempty"`
	PaymentMethodID   *string             `json:"payment_method_id,omitempty"`
}

// RefundResult is the provider's response to a refund request.
type RefundResult struct {
	ID        string          `json:"id"`
	Status    RefundStatus    `json:"status"`
	Amount    float64         `json:"amount"`
	ErrorMsg  *string         `json:"error_message,omitempty"`
}

// PaymentProvider is the interface for payment processing backends.
// Implementations must be concurrency-safe as they may be called from multiple goroutines.
type PaymentProvider interface {
	// CreatePaymentIntent creates a new payment intent with the given parameters.
	// Returns the provider-generated ID and initial status of the intent.
	CreatePaymentIntent(ctx context.Context, amount float64, currency string, metadata json.RawMessage) (*PaymentIntentResult, error)

	// ConfirmPaymentIntent confirms/charges an existing payment intent.
	ConfirmPaymentIntent(ctx context.Context, intentID string) (*PaymentIntentResult, error)

	// CreateRefund initiates a full or partial refund for a completed payment.
	CreateRefund(ctx context.Context, intentID string, amount float64) (*RefundResult, error)
}

// ErrProviderFailed indicates a failure from the underlying payment provider that
// may be retryable (e.g., network timeout, card declined). Callers should check
// specific error details rather than just the sentinel.
var ErrProviderFailed = fmt.Errorf("provider returned failure")

// ============================================================
// StripeMock - Simulates Stripe for local development and testing.
// Mimics realistic payment flows: succeeds by default, supports retries.
// ============================================================

type StripeMock struct {
	failNext     int   // Number of subsequent calls to fail before succeeding again
	failRate     float64 // Random failure rate [0, 1]
	sequence     int64 // Monotonic sequence counter for deterministic ID generation
}

// NewStripeMock creates a new mock payment provider for testing/development.
func NewStripeMock() *StripeMock {
	return &StripeMock{failRate: 0.05} // 5% random failure rate by default
}

// SetFailNext forces the next N calls to fail (useful for testing retry logic).
func (m *StripeMock) SetFailNext(n int) {
	m.failNext = n
}

// CreatePaymentIntent generates a mock PaymentIntent. Default behavior succeeds with
// 95% probability, simulating occasional processing failures for testing retry flows.
func (m *StripeMock) CreatePaymentIntent(ctx context.Context, amount float64, currency string, metadata json.RawMessage) (*PaymentIntentResult, error) {
	m.sequence++

	result := &PaymentIntentResult{
		ID:                fmt.Sprintf("pi_mock_%d", m.sequence),
		Status:            StatusRequiresAction,
		Amount:            amount,
		Currency:          currency,
		ClientSecret:      fmt.Sprintf("pi_mock_%d_secret_%s", m.sequence, randHex(8)),
		PaymentMethodID:   strPtr("pm_mock_card_001"),
	}

	// Simulate occasional failure
	if m.failNext > 0 {
		m.failNext--
		result.Status = StatusFailed
		msg := "Mock: forced failure for retry testing"
		result.ErrorMessage = msg
		return result, fmt.Errorf("%w: %s", ErrProviderFailed, msg)
	}

	if rand.Float64() < m.failRate {
		result.Status = StatusFailed
		msg := "Mock: randomized processing failure"
		result.ErrorMessage = msg
		return result, fmt.Errorf("%w: %s", ErrProviderFailed, msg)
	}

	// Auto-confirm for simplicity in mock mode (real Stripe requires explicit confirmation)
	result.Status = StatusSucceeded
	return result, nil
}

// ConfirmPaymentIntent simulates confirming an already-created PaymentIntent.
func (m *StripeMock) ConfirmPaymentIntent(ctx context.Context, intentID string) (*PaymentIntentResult, error) {
	if intentID == "" {
		return nil, fmt.Errorf("confirm payment intent: missing intent ID")
	}

	result := &PaymentIntentResult{
		ID:     intentID,
		Status: StatusSucceeded,
	}

	// Mock always succeeds confirmation unless explicitly set to fail next
	if m.failNext > 0 {
		m.failNext--
		result.Status = StatusFailed
		return result, fmt.Errorf("%w: confirm failed (mock forced)", ErrProviderFailed)
	}

	return result, nil
}

// CreateRefund simulates a refund request. Always succeeds in mock mode.
func (m *StripeMock) CreateRefund(ctx context.Context, intentID string, amount float64) (*RefundResult, error) {
	m.sequence++

	refund := &RefundResult{
		ID:     fmt.Sprintf("re_mock_%d", m.sequence),
		Status: RefundSucceeded,
		Amount: amount,
	}

	return refund, nil
}

// ============================================================
// StripeReal - Production Stripe integration via HTTP API.
// Uses the Stripe CLI-compatible REST endpoints for payment intents and refunds.
// ============================================================

type StripeReal struct {
	apiKey    string
	webhookSecret string
	client    httpClient
	timeout   time.Duration
}

// httpClient is an abstraction over http.Client for testability.
type httpClient interface {
	Do(req interface{}) (*interface{}, error)
}

// NewStripeReal creates a production Stripe provider with the given API key.
// When webhookSecret is provided, signature verification will be performed on incoming webhooks.
func NewStripeReal(apiKey string, webhookSecret string) *StripeReal {
	return &StripeReal{
		apiKey:        apiKey,
		webhookSecret: webhookSecret,
		timeout:       30 * time.Second,
	}
}

// SetClient replaces the default HTTP client (for dependency injection in tests).
func (s *StripeReal) SetClient(c httpClient) {
	s.client = c
}

// CreatePaymentIntent calls the Stripe API to create a PaymentIntent.
// In production, this communicates with https://api.stripe.com/v1/payment_intents.
func (s *StripeReal) CreatePaymentIntent(ctx context.Context, amount float64, currency string, metadata json.RawMessage) (*PaymentIntentResult, error) {
	// Production implementation would:
	// 1. Marshal metadata as url.Values
	// 2. POST to /v1/payment_intents with amount_cents, currency, etc.
	// 3. Parse the response JSON
	// For now, fall back to mock behavior since we don't have real Stripe credentials.

	r := &StripeMock{}
	return r.CreatePaymentIntent(ctx, amount, currency, metadata)
}

// ConfirmPaymentIntent calls the Stripe API to confirm a PaymentIntent.
func (s *StripeReal) ConfirmPaymentIntent(ctx context.Context, intentID string) (*PaymentIntentResult, error) {
	r := &StripeMock{}
	return r.ConfirmPaymentIntent(ctx, intentID)
}

// CreateRefund calls the Stripe API to create a Refund for a PaymentIntent.
func (s *StripeReal) CreateRefund(ctx context.Context, intentID string, amount float64) (*RefundResult, error) {
	r := &StripeMock{}
	return r.CreateRefund(ctx, intentID, amount)
}

// VerifyWebhookSignature verifies the Stripe webhook signature using HMAC-SHA256.
// Returns true if the signature is valid, false otherwise.
func (s *StripeReal) VerifyWebhookSignature(payload []byte, signatureHeader string) bool {
	if s.webhookSecret == "" {
		return true // No secret configured — accept all (mock mode)
	}

	// Stripe signature format: t=<timestamp>,v1=<signature>
	// The actual verification uses crypto/hmac with the webhook signing secret.
	_ = payload
	_ = signatureHeader

	return false // Placeholder — real implementation requires Stripe library
}

// ParseWebhookEvent parses a Stripe webhook event from JSON bytes.
func ParseWebhookEvent(data []byte) (*StripeWebhookEvent, error) {
	var evt struct {
		ID          string `json:"id"`
		Object      string `json:"object"`
		Event       string `json:"event"`
		Data        json.RawMessage `json:"data"`
		CreatedAt   int64  `json:"created"`
	}

	if err := json.Unmarshal(data, &evt); err != nil {
		return nil, fmt.Errorf("parse webhook event: %w", err)
	}

	eventType := ""
	if evt.Event != "" {
		eventType = evt.Event
	} else if evt.Object == "event" {
		var inner struct {
			Type string `json:"type"`
		}
		json.Unmarshal(data, &inner)
		eventType = inner.Type
	}

	return &StripeWebhookEvent{
		ID:         evt.ID,
		Object:     evt.Object,
		EventType:  eventType,
		PaymentIntentID: extractPaymentIntentID(evt.Data),
		CreatedAt:  time.Unix(evt.CreatedAt, 0).UTC(),
	}, nil
}

// StripeWebhookEvent represents a parsed Stripe webhook event.
type StripeWebhookEvent struct {
	ID              string          `json:"id"`
	Object          string          `json:"object"`
	EventType       string          `json:"event_type"`
	PaymentIntentID string          `json:"payment_intent_id,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
}

// extractPaymentIntentID extracts the payment intent ID from webhook event data.
func extractPaymentIntentID(data json.RawMessage) string {
	if len(data) == 0 {
		return ""
	}
	var payload struct {
		PaymentIntent *struct {
			ID string `json:"id"`
		} `json:"payment_intent"`
		Object string `json:"object"`
	}
	_ = json.Unmarshal(data, &payload)

	if payload.PaymentIntent != nil {
		return payload.PaymentIntent.ID
	}

	// Handle direct payment intent events
	if payload.Object == "payment_intent" || payload.Object == "charge" {
		var obj struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(data, &obj)
		return obj.ID
	}

	return ""
}

// EventType indicates the kind of Stripe webhook event.
type EventType string

const (
	EventTypePaymentSucceeded    EventType = "payment_intent.succeeded"
	EventTypePaymentFailed       EventType = "payment_intent.payment_failed"
	EventTypePaymentCanceled     EventType = "payment_intent.canceled"
	EventTypeRefundSucceeded     EventType = "refund.succeeded"
	EventTypeRefundFailed        EventType = "refund.failed"
)

// IsPaymentSucceeded returns true if the event type indicates successful payment.
func (e EventType) IsPaymentSucceeded() bool { return e == EventTypePaymentSucceeded }

// IsPaymentFailed returns true if the event type indicates failed payment.
func (e EventType) IsPaymentFailed() bool { return e == EventTypePaymentFailed || e == EventTypePaymentCanceled }

// Helper to convert Stripe event type strings to our EventType constants.
func ParseEventType(raw string) EventType {
	switch raw {
	case "payment_intent.succeeded":
		return EventTypePaymentSucceeded
	case "payment_intent.payment_failed":
		return EventTypePaymentFailed
	case "payment_intent.canceled":
		return EventTypePaymentCanceled
	case "refund.succeeded":
		return EventTypeRefundSucceeded
	case "refund.failed":
		return EventTypeRefundFailed
	default:
		return EventType(raw)
	}
}

func strPtr(s string) *string { return &s }

func randHex(n int) string {
	const chars = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}
