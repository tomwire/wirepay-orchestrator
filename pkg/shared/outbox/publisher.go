// Package outbox implements the transactional outbox pattern for WPO services.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// Event represents an outbox event row stored in the database before being published to Kafka.
type Event struct {
	ID           string          `json:"id"`
	EventType    string          `json:"event_type"`
	ServiceName  string          `json:"service_name"`
	Key          string          `json:"key"`
	Payload      json.RawMessage `json:"payload"`
	CreatedAt    time.Time       `json:"created_at"`
	PublishedAt  *time.Time      `json:"published_at,omitempty"`
	Status       string          `json:"status"` // "pending", "published", "failed"
	RetryCount   int             `json:"retry_count"`
	ErrorMessage *string         `json:"error_message,omitempty"`
}

// Publisher manages the outbox table and periodic Kafka publishing.
type Publisher struct {
	db          *pgxpool.Pool
	topic       string
	serviceName string
	publishFn   PublishFunc
	interval    time.Duration
	batchSize   int
	logger      *zap.Logger
	cancel      context.CancelFunc
	ctx         context.Context
}

// PublishFunc is the interface for sending an outbox event to Kafka.
type PublishFunc func(ctx context.Context, topic string, key []byte, payload []byte) error

// NewPublisher creates a new Outbox publisher that polls every `interval` and publishes in batches of `batchSize`.
func NewPublisher(db *pgxpool.Pool, serviceName, topic string, interval time.Duration, batchSize int, publishFn PublishFunc, logger *zap.Logger) *Publisher {
	return &Publisher{
		db:          db,
		topic:       topic,
		serviceName: serviceName,
		publishFn:   publishFn,
		interval:    interval,
		batchSize:   batchSize,
		logger:      logger,
	}
}

// Start begins the background polling loop. Blocks until context is cancelled.
func (p *Publisher) Start(ctx context.Context) {
	p.ctx, p.cancel = context.WithCancel(ctx)

	go func() {
		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		for {
			select {
			case <-p.ctx.Done():
				p.logger.Info("outbox publisher stopping")
				return
			case <-ticker.C:
				if err := p.flush(); err != nil {
					p.logger.Error("outbox flush failed", zap.Error(err))
				}
			}
		}
	}()

	log.Printf("[outbox] publisher started for %s (topic=%s, interval=%s)", p.serviceName, p.topic, p.interval)
}

// Stop signals the publisher to stop.
func (p *Publisher) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

// WriteToOutbox inserts an event into the outbox table within a transaction.
func (p *Publisher) WriteToOutbox(ctx context.Context, eventType string, key string, payload interface{}) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}

	eventID := generateEventID()

	const insertSQL = `
		INSERT INTO outbox_events (id, event_type, service_name, "key", payload, status)
		VALUES ($1, $2, $3, $4, $5, 'pending')
		ON CONFLICT DO NOTHING
	`

	_, err = p.db.Exec(ctx, insertSQL, eventID, eventType, p.serviceName, key, payloadBytes)
	if err != nil {
		return fmt.Errorf("insert into outbox: %w", err)
	}

	p.logger.Debug("event written to outbox",
		zap.String("id", eventID),
		zap.String("event_type", eventType),
		zap.String("key", key))

	return nil
}

// flush polls for pending events and publishes them to Kafka.
func (p *Publisher) flush() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := p.db.Query(ctx, `
		SELECT id, event_type, "key", payload FROM outbox_events 
		WHERE status = 'pending' AND retry_count < 5
		ORDER BY created_at ASC LIMIT $1
	`, p.batchSize)
	if err != nil {
		return fmt.Errorf("outbox flush query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var e Event
		var payload json.RawMessage
		err := rows.Scan(&e.ID, &e.EventType, &e.Key, &payload)
		if err != nil {
			p.logger.Error("outbox scan error", zap.Error(err))
			continue
		}

		e.Payload = payload

		if err := p.publishFn(ctx, p.topic, []byte(e.Key), e.Payload); err != nil {
			p.logger.Error("outbox publish failed", zap.String("id", e.ID), zap.Error(err))
			continue
		}

		now := time.Now()
		if _, err := p.db.Exec(ctx, `UPDATE outbox_events SET status = 'published', published_at = $1 WHERE id = $2`, now, e.ID); err != nil {
			p.logger.Error("outbox update failed", zap.String("id", e.ID), zap.Error(err))
		}
	}

	return rows.Err()
}

func generateEventID() string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = byte(time.Now().UnixNano() >> uint(i*8))
	}
	return fmt.Sprintf("%x", b[:8])
}
