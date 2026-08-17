package repository

import (
	"context"
	"fmt"
	"time"
)

// NotificationRecord represents a notification row in the database.
type NotificationRecord struct {
	ID          string     `json:"id"`
	OrderID     string     `json:"order_id,omitempty"`
	EventType   string     `json:"event_type"`
	TemplateID  string     `json:"template_id,omitempty"`
	Subject     string     `json:"subject,omitempty"`
	BodyHTML    string     `json:"body_html,omitempty"`
	Status      string     `json:"status"` // PENDING, DELIVERING, DELIVERED, FAILED, DLQ
	Channel     string     `json:"delivery_channel"`
	RetryCount  int        `json:"retry_count"`
	ErrorMsg    string     `json:"error_message,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	DeliveredAt *time.Time `json:"delivered_at,omitempty"`
}

// TemplateRecord represents a notification template row.
type TemplateRecord struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	EventType        string    `json:"event_type"`
	SubjectTemplate  string    `json:"subject_template"`
	BodyHTMLOrHTML   string    `json:"body_html_template"`
	IsActive         bool      `json:"is_active"`
	CreatedAt        time.Time `json:"created_at"`
}

// NotificationRepository handles persistence operations.
type NotificationRepository struct {
	db interface{} // placeholder DB handle
}

// NewNotificationRepository creates a new repository.
func NewNotificationRepository(db interface{}) *NotificationRepository {
	return &NotificationRepository{db: db}
}

// CreateNotification inserts a new notification record.
func (r *NotificationRepository) CreateNotification(ctx context.Context, n *NotificationRecord) error {
	_ = ctx
	fmt.Println("create notification placeholder")
	return nil
}

// GetDLQEvents retrieves failed notifications for dead-letter processing.
func (r *NotificationRepository) GetDLQEvents(ctx context.Context, limit int) ([]*NotificationRecord, error) {
	_ = ctx
	return []*NotificationRecord{}, nil
}

// UpdateStatus updates the status of a notification record.
func (r *NotificationRepository) UpdateStatus(ctx context.Context, id string, status string) error {
	_ = ctx
	fmt.Printf("update status %s -> %s\n", id, status)
	return nil
}

// UpsertTemplate inserts or updates a template record.
func (r *NotificationRepository) UpsertTemplate(ctx context.Context, tmpl *TemplateRecord) error {
	_ = ctx
	_ = tmpl
	fmt.Println("upsert template placeholder")
	return nil
}

// GetTemplateByEventType retrieves an active template by event type.
func (r *NotificationRepository) GetTemplateByEventType(ctx context.Context, eventType string) (*TemplateRecord, error) {
	_ = ctx
	fmt.Printf("get template by event type: %s\n", eventType)
	return &TemplateRecord{}, nil
}
