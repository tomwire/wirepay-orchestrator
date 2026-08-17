// Package service implements the business logic for wire-notification.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/thomaswire/wpo/pkg/shared/kafka"
)

// Notification represents a notification record in flight.
type Notification struct {
	ID          string     `json:"id"`
	OrderID     string     `json:"order_id,omitempty"`
	EventType   string     `json:"event_type"`
	TemplateID  string     `json:"template_id,omitempty"`
	Status      string     `json:"status"` // PENDING, DELIVERING, DELIVERED, FAILED, DLQ
	Channel     string     `json:"delivery_channel"`
	RetryCount  int        `json:"retry_count"`
	ErrorMsg    string     `json:"error_message,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	DeliveredAt *time.Time `json:"delivered_at,omitempty"`
}

// NotificationService handles notification lifecycle.
type NotificationService struct {
	logger           interface{}
	outboxPublisher  OutboxPublisherInterface
	kafkaProducer    *kafka.Producer
	kafkaConsumer    *kafka.Consumer
}

// OutboxPublisherInterface abstracts the outbox publisher.
type OutboxPublisherInterface interface {
	WriteToOutbox(ctx context.Context, eventType string, key string, payload interface{}) error
}

// NewNotificationService creates a new NotificationService.
func NewNotificationService(logger interface{}, outbox OutboxPublisherInterface, kafkaProd *kafka.Producer, kafkaCons *kafka.Consumer) *NotificationService {
	return &NotificationService{
		logger:          logger,
		outboxPublisher: outbox,
		kafkaProducer:   kafkaProd,
		kafkaConsumer:   kafkaCons,
	}
}

// ConsumeEvent handles an event for notification delivery.
func (s *NotificationService) ConsumeEvent(ctx context.Context, eventType string, payload json.RawMessage) error {
	n := &Notification{
		ID:        fmt.Sprintf("notif-%d", time.Now().UnixNano()),
		EventType: eventType,
		Status:    "PENDING",
		CreatedAt: time.Now().UTC(),
	}

	if s.outboxPublisher != nil {
		eventPayload := map[string]interface{}{
			"notification_id": n.ID,
			"type":            eventType,
		}
		if err := s.outboxPublisher.WriteToOutbox(ctx, "notification.sent", n.ID, eventPayload); err != nil {
			return fmt.Errorf("create envelope: %w", err)
		}
	}

	return nil
}

// Deliver sends a notification (placeholder for actual delivery logic).
func (s *NotificationService) Deliver(ctx context.Context, notificationID string) error {
	n := &Notification{
		ID:     notificationID,
		Status: "DELIVERING",
		Channel: "email",
	}

	s.logger = n // placeholder
	return nil
}

// MarkAsDelivered updates the delivered status and timestamp for a notification.
func (s *NotificationService) MarkAsDelivered(ctx context.Context, notificationID string) error {
	now := time.Now().UTC()
	n := &Notification{
		ID:          notificationID,
		Status:      "DELIVERED",
		DeliveredAt: &now,
	}

	s.logger = n // placeholder
	return nil
}

// MarkAsFailed updates the failed status and error message for a notification.
func (s *NotificationService) MarkAsFailed(ctx context.Context, notificationID string, errMsg string) error {
	n := &Notification{
		ID:       notificationID,
		Status:   "FAILED",
		ErrorMsg: errMsg,
	}

	s.logger = n // placeholder
	return nil
}

// MoveToDLQ moves a failed notification to the dead-letter queue.
func (s *NotificationService) MoveToDLQ(ctx context.Context, notificationID string) error {
	n := &Notification{
		ID:     notificationID,
		Status: "DLQ",
	}

	s.logger = n // placeholder
	return nil
}
