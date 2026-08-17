// Package kafka provides shared Kafka producer/consumer helpers for WPO services.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

// Producer wraps Sarama's sync producer with JSON serialization and tracing headers.
type Producer struct {
	client  sarama.SyncProducer
	logger  *zap.Logger
	topics  []string
	brokers string
}

// NewSyncProducer creates a new synchronous Kafka producer.
func NewSyncProducer(logger *zap.Logger, brokers string, topics ...string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Return.Successes = true
	cfg.Producer.MaxMessageBytes = 1048576 // 1MB

	client, err := sarama.NewSyncProducer([]string{brokers}, cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka sync producer: %w", err)
	}

	logger.Info("kafka producer connected", zap.Strings("topics", topics), zap.String("brokers", brokers))

	return &Producer{
		client:  client,
		logger:  logger,
		topics:  topics,
		brokers: brokers,
	}, nil
}

// Publish sends a JSON-serialized event to a topic with optional key and tracing.
func (p *Producer) Publish(ctx context.Context, topic string, key []byte, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal kafka message: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(data),
		Timestamp: time.Now(),
	}

	tracer := otel.Tracer("wirepay/kafka")
	ctx, span := tracer.Start(ctx, fmt.Sprintf("kafka.publish.%s", topic))
	defer span.End()

	propagator := otel.GetTextMapPropagator()
	carrier := &saramaKafkaCarrier{msg: msg}
	propagator.Inject(ctx, carrier)

	partition, offset, err := p.client.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("kafka publish to %s: %w", topic, err)
	}

	p.logger.Debug("published event",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
		zap.String("key", string(key)))

	return nil
}

// Close shuts down the Kafka producer and waits for pending messages.
func (p *Producer) Close() error {
	err := p.client.Close()
	p.logger.Info("kafka producer closed")
	return err
}

// PublishEvent sends a structured event to a topic with tracing headers.
func (p *Producer) PublishEvent(ctx context.Context, topic string, key []byte, payload interface{}) error {
	return p.Publish(ctx, topic, key, payload)
}

// Consumer wraps a Sarama consumer group.
type Consumer struct {
	consumer sarama.ConsumerGroup
	logger   *zap.Logger
	groupID  string
	topics   []string
	brokers  string
	handlers map[string]HandlerFunc
	mu       sync.Mutex
}

// HandlerFunc processes a single Kafka message.
type HandlerFunc func(ctx context.Context, key []byte, value []byte) error

// NewConsumerGroup creates a consumer group that subscribes to the given topics.
func NewConsumerGroup(logger *zap.Logger, brokers, groupID string, topics ...string) (*Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	cg, err := sarama.NewConsumerGroup([]string{brokers}, groupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer group: %w", err)
	}

	logger.Info("consumer group joined", zap.String("group", groupID), zap.Strings("topics", topics))

	return &Consumer{
		consumer: cg,
		groupID:  groupID,
		topics:   topics,
		brokers:  brokers,
		logger:   logger,
		handlers: make(map[string]HandlerFunc),
	}, nil
}

// Subscribe registers a handler for the given topic.
func (c *Consumer) Subscribe(topic string, handler HandlerFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[topic] = handler
}

// Run starts consuming messages from all subscribed topics. Blocks until context is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			var consumerGroupHandler ConsumerGroupHandler
			err := c.consumer.Consume(ctx, c.topics, &consumerGroupHandler)

			if err != nil {
				c.logger.Error("kafka consumer error", zap.Error(err))
				time.Sleep(1 * time.Second)
				continue
			}

			if ctx.Err() != nil {
				return nil
			}
		}
	}
}

// Close shuts down the consumer group.
func (c *Consumer) Close() error {
	err := c.consumer.Close()
	c.logger.Info("consumer group closed")
	return err
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler for message processing.
type ConsumerGroupHandler struct {
	consumer *Consumer
}

// Setup is called once at session creation.
func (h *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.consumer.logger.Debug("consumer group setup")
	return nil
}

// Cleanup is called once at session teardown.
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	h.consumer.logger.Debug("consumer group cleanup")
	return nil
}

// ConsumeClaim processes messages for a single topic partition.
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		key := []byte(string(msg.Key))
		topic := msg.Topic

		h.consumer.mu.Lock()
		handler, ok := h.consumer.handlers[topic]
		h.consumer.mu.Unlock()

		if !ok {
			h.consumer.logger.Warn("no handler for topic", zap.String("topic", topic))
			session.MarkMessage(msg, "")
			continue
		}

		err := handler(session.Context(), key, msg.Value)
		if err != nil {
			h.consumer.logger.Error("consumer handler failed",
				zap.String("topic", topic),
				zap.String("key", string(key)),
				zap.Error(err))
			session.MarkMessage(msg, "")
			continue
		}

		session.MarkMessage(msg, "")
	}

	return nil
}

// saramaKafkaCarrier implements the W3C propagation carrier for Kafka headers.
type saramaKafkaCarrier struct {
	msg *sarama.ProducerMessage
}

func (c saramaKafkaCarrier) Get(key string) string {
	for _, h := range c.msg.Headers {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *saramaKafkaCarrier) Set(key, val string) {
	c.msg.Headers = append(c.msg.Headers, sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(val),
	})
}

func (c saramaKafkaCarrier) Keys() []string {
	keys := make([]string, len(c.msg.Headers))
	for i, h := range c.msg.Headers {
		keys[i] = string(h.Key)
	}
	return keys
}

// EventEnvelope is a standardized envelope for events published to Kafka.
type EventEnvelope struct {
	EventType   string                 `json:"event_type"`
	ServiceName string                 `json:"service_name"`
	ID          string                 `json:"id,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	Payload     json.RawMessage        `json:"payload"`
	CreatedAt   time.Time              `json:"created_at"`
	TraceID     string                 `json:"trace_id,omitempty"`
}

// NewEventEnvelope creates a new EventEnvelope with the given parameters.
func NewEventEnvelope(eventType, serviceName, id string, payload interface{}) (*EventEnvelope, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope payload: %w", err)
	}

	return &EventEnvelope{
		EventType:   eventType,
		ServiceName: serviceName,
		ID:          id,
		Metadata:    make(map[string]interface{}),
		Payload:     data,
		CreatedAt:   time.Now().UTC(),
	}, nil
}
