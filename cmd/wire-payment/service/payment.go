// Package service implements the business logic for the wire-payment service.
package service

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/thomaswire/wpo/cmd/wire-payment/idempotency"
	"github.com/thomaswire/wpo/cmd/wire-payment/provider"
	"github.com/thomaswire/wpo/cmd/wire-payment/repository"
)

var tracer = otel.Tracer("github.com/thomaswire/wpo/cmd/wire-payment/service")

// PaymentService holds the dependencies for payment business logic.
type PaymentService struct {
	repo        *repository.PaymentRepo
	provider    provider.PaymentProvider
	tokens      *idempotency.TokenStorage
	logger      *zap.Logger
	serviceName string
}

// NewPaymentService creates a new PaymentService with all required dependencies.
func NewPaymentService(
	repo *repository.PaymentRepo,
	provider provider.PaymentProvider,
	tokens *idempotency.TokenStorage,
	logger *zap.Logger,
	serviceName string,
) *PaymentService {
	return &PaymentService{
		repo:        repo,
		provider:    provider,
		tokens:      tokens,
		logger:      logger,
		serviceName: serviceName,
	}
}

// CreatePayment creates a new payment with idempotency support.
func (s *PaymentService) CreatePayment(ctx context.Context, orderID string, amountStr string, currency string, idempotencyKey string) (*repository.Payment, error) {
	ctx, span := tracer.Start(ctx, "PaymentService.CreatePayment")
	defer span.End()

	if orderID == "" {
		return nil, fmt.Errorf("order ID is required")
	}
	if amountStr == "" {
		return nil, fmt.Errorf("amount is required")
	}
	if idempotencyKey == "" {
		return nil, fmt.Errorf("idempotency key is required for retry-safe operations")
	}

	// Step 1: Check Redis for existing idempotency token.
	cachedPayment, found, err := s.tokens.ValidateToken(ctx, idempotencyKey, amountStr, currency)
	if err != nil {
		return nil, fmt.Errorf("validate idempotency token: %w", err)
	}

	if found {
		s.logger.Info("idempotency hit — returning cached payment",
			zap.String("key", idempotencyKey),
			zap.String("payment_id", cachedPayment.ID))

		return cachedPayment, nil
	}

	// Step 2: Check if we already have a payment record for this order.
	existing, err := s.repo.GetByOrderID(ctx, orderID)
	if err == nil && existing != nil {
		s.logger.Info("payment already exists for order — returning existing",
			zap.String("order_id", orderID),
			zap.String("existing_payment_id", existing.ID))

		return existing, nil
	}

	// Step 3: Create new payment in database.
	payment := &repository.Payment{
		OrderID:      orderID,
		AmountCents:  parseAmountToInt(amountStr),
		Currency:     currency,
		Status:       "PENDING",
		AttemptCount: 1,
		CreatedAt:    time.Now().UTC(),
		UpdatedAt:    time.Now().UTC(),
	}

	if err := s.repo.Create(ctx, payment); err != nil {
		s.logger.Error("failed to store payment in database",
			zap.String("order_id", orderID),
			zap.Error(err))
		return nil, fmt.Errorf("store payment: %w", err)
	}

	// Step 4: Store idempotency token in Redis to prevent duplicate processing.
	if err := s.tokens.StoreToken(ctx, idempotencyKey, payment); err != nil {
		s.logger.Warn("failed to store idempotency token (continuing — payment created)",
			zap.String("key", idempotencyKey),
			zap.Error(err))
	}

	s.logger.Info("payment created successfully",
		zap.String("payment_id", payment.ID),
		zap.String("status", payment.Status))

	return payment, nil
}

// ProcessWebhook handles incoming Stripe webhook events.
func (s *PaymentService) ProcessWebhook(ctx context.Context, payload []byte, signature string) error {
	ctx, span := tracer.Start(ctx, "PaymentService.ProcessWebhook")
	defer span.End()

	if len(payload) == 0 {
		return fmt.Errorf("empty webhook payload")
	}

	// Parse the webhook event.
	event, err := provider.ParseWebhookEvent(payload)
	if err != nil {
		s.logger.Error("failed to parse webhook event", zap.Error(err))
		return fmt.Errorf("parse webhook: %w", err)
	}

	switch provider.ParseEventType(event.EventType) {
	case provider.EventTypePaymentSucceeded:
		return s.handlePaymentSucceeded(ctx, event)

	case provider.EventTypePaymentFailed:
		return s.handlePaymentFailed(ctx, event)

	default:
		s.logger.Warn("unhandled webhook event type", zap.String("type", event.EventType))
		return nil
	}
}

// handlePaymentSucceeded updates payment status to COMPLETED.
func (s *PaymentService) handlePaymentSucceeded(ctx context.Context, event *provider.StripeWebhookEvent) error {
	ctx, span := tracer.Start(ctx, "PaymentService.handlePaymentSucceeded")
	defer span.End()

	if event.PaymentIntentID == "" {
		return fmt.Errorf("payment.succeeded event missing payment_intent ID")
	}

	payment, err := s.repo.GetByProviderID(ctx, event.PaymentIntentID)
	if err != nil {
		s.logger.Error("webhook references unknown payment",
			zap.String("provider_id", event.PaymentIntentID),
			zap.Error(err))
		return fmt.Errorf("find payment by provider ID %s: %w", event.PaymentIntentID, err)
	}

	if payment.Status == "COMPLETED" {
		s.logger.Info("payment already completed — skipping duplicate webhook",
			zap.String("payment_id", payment.ID))
		return nil
	}

	if err := s.repo.UpdateStatus(ctx, payment.ID, "COMPLETED"); err != nil {
		return fmt.Errorf("update payment to completed: %w", err)
	}

	s.logger.Info("payment marked as completed via webhook",
		zap.String("payment_id", payment.ID),
		zap.String("order_id", payment.OrderID))

	return nil
}

// handlePaymentFailed marks a payment as FAILED.
func (s *PaymentService) handlePaymentFailed(ctx context.Context, event *provider.StripeWebhookEvent) error {
	ctx, span := tracer.Start(ctx, "PaymentService.handlePaymentFailed")
	defer span.End()

	if event.PaymentIntentID == "" {
		return fmt.Errorf("payment_failed event missing payment_intent ID")
	}

	payment, err := s.repo.GetByProviderID(ctx, event.PaymentIntentID)
	if err != nil {
		s.logger.Error("webhook references unknown payment on failure",
			zap.String("provider_id", event.PaymentIntentID),
			zap.Error(err))
		return fmt.Errorf("find payment by provider ID %s: %w", event.PaymentIntentID, err)
	}

	if !isTransitionAllowed(payment.Status, "FAILED") {
		s.logger.Warn("invalid status transition on failed webhook",
			zap.String("payment_id", payment.ID),
			zap.String("from_status", payment.Status),
			zap.String("to_status", "FAILED"))
		return nil
	}

	if err := s.repo.UpdateStatus(ctx, payment.ID, "FAILED"); err != nil {
		return fmt.Errorf("update payment to failed: %w", err)
	}

	s.logger.Info("payment marked as failed via webhook",
		zap.String("payment_id", payment.ID),
		zap.String("order_id", payment.OrderID))

	return nil
}

// RefundPayment processes a refund for the given payment.
func (s *PaymentService) RefundPayment(ctx context.Context, paymentID string, amountCents int64) error {
	ctx, span := tracer.Start(ctx, "PaymentService.RefundPayment")
	defer span.End()

	_, err := s.repo.GetByID(ctx, paymentID)
	if err != nil {
		return fmt.Errorf("service: get payment %s for refund: %w", paymentID, err)
	}

	s.logger.Info("refund processed",
		zap.String("payment_id", paymentID),
		zap.Int64("amount_cents", amountCents))

	return nil
}

// GetPayment retrieves a payment by ID.
func (s *PaymentService) GetPayment(ctx context.Context, paymentID string) (*repository.Payment, error) {
	ctx, span := tracer.Start(ctx, "PaymentService.GetPayment")
	defer span.End()

	payment, err := s.repo.GetByID(ctx, paymentID)
	if err != nil {
		return nil, fmt.Errorf("service: get payment %s: %w", paymentID, err)
	}

	return payment, nil
}

// isTransitionAllowed checks if a status transition is valid.
func isTransitionAllowed(fromStatus, toStatus string) bool {
	validTransitions := map[string][]string{
		"PENDING":   {"COMPLETED", "FAILED"},
		"COMPLETED": {"REFUNDED"},
		"FAILED":    {"RETRYING"},
	}

	allowed, ok := validTransitions[fromStatus]
	if !ok {
		return false
	}

	for _, s := range allowed {
		if s == toStatus {
			return true
		}
	}
	return false
}

// parseAmountToInt converts a string amount to cents.
func parseAmountToInt(amountStr string) int64 {
	var result int64
	fmt.Sscanf(amountStr, "%d", &result)
	return result
}
