# WirePay Orchestrator (WPO)

A production-grade, distributed event-driven checkout and payment orchestration engine — modeled after modern enterprise headless systems like commercetools and Stripe. Built for high concurrency, resilient distributed state management, and strict financial auditability across isolated microservices.

> **Purpose:** Demonstrates enterprise-grade microservices architecture: transactional outbox patterns, Saga orchestration with compensation, Redis Redlock distributed locking, Kafka event streaming, double-entry accounting ledgers, full OpenTelemetry observability, and production Kubernetes deployments — all in a single cohesive system.

[![Go](https://img.shields.io/badge/Go-1.22+-00ADD8.svg?style=flat&logo=go)](https://golang.org/)
[![Kafka](https://img.shields.io/badge/Kafka-Apache_2.4+-231F20.svg?style=flat&logo=apache-kafka)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16+-336791.svg?style=flat&logo=postgresql)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7.2+-DC382D.svg?style=flat&logo=redis)](https://redis.io/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Table of Contents

- [Architecture](#architecture)
- [Services](#services)
- [Key Patterns](#key-patterns)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start (Local Dev)](#quick-start-local-dev)
- [Production Deployment](#production-deployment)
- [Load Testing](#load-testing)
- [Chaos Engineering](#chaos-engineering)
- [Observability](#observability)
- [Development Workflow](#development-workflow)
- [Configuration](#configuration)

---

## Architecture

```
                            ┌─────────────────────────────────────────────────────────┐
                            │                  WirePay Orchestrator                    │
                            │                         Kafka Topic Layer                │
                            │                                                           │
                            │  order.created │ inventory.reserved │ payment.succeeded   │
                            │  order.failed  │ inventory.released │ payment.refunded    │
                            └─────────────────┬──────────────┬─────────────────────────┘
                                              │              │ Kafka (KRaft Mode)
                        ┌─────────────────────┼──────────────┼──────────────────────┐
                        │                     │              │                      │
           ┌────────────▼────┐    ┌────────────▼────┐  ┌──────▼──────┐  ┌───────────▼──────────┐
           │ wire-order       │    │ wire-inventory   │  │wire-payment │  │ wire-notification     │
           │ :8081            │    │ :8082            │  │:8083        │  │:8084                  │
           │ (Order Service)  │    │ (Inventory Svc)  │  │(Payment Svc)│  │(Notification Svc)     │
           └────────────┬────┘    └────────────┬─────┘  └──────┬──────┘  └──────────┬───────────┘
                        │                     │               │                    │
           ┌────────────▼─────────────────────▼───────────────▼────────────────────▼────┐
           │                                  PostgreSQL Pools                                                         │
           │  wire_order   │   wire_inventory  │  wire_payment  │  wire_notification   │  wire_ledger              │
           └───────────────────────────────────────────────────────────────────────────────────────────────────────┘
                        ▲                          ▲                    ▲                      ▲
                        │   Redis Cluster          │   Circuit          │   Templates            │
           ┌────────────▼────────────────────────────────────────────────────────────────────▼────┐
           │  wire-inventory: Redlock distributed locks, stock reservation state                 │
           │  wire-order: Checkout session cache, rate limiting                                   │
           │  wire-payment: Idempotency token store (key-value)                                  │
           │  All services: Outbox polling table for CDC publish                                  │
           └─────────────────────────────────────────────────────────────────────────────────────┘

         Kafka Events Flow: order.created → inventory.reserved → payment.succeeded → ledger.posted → notification.sent
                                                  ↓ compensate (saga rollback)
                                            inventory.released + payment.refunded
```

### Data Flow — Happy Path (Checkout)

1. **wire-order** receives `POST /checkout` → creates Order, publishes `order.created` to Kafka outbox
2. **wire-inventory** consumes `order.created` → acquires distributed lock via Redlock, decrements stock atomically, publishes `inventory.reserved`
3. **wire-payment** consumes `inventory.reserved` → creates Stripe PaymentIntent, publishes `payment.succeeded`
4. **wire-ledger** consumes `payment.succeeded` → appends immutable double-entry ledger records, publishes `ledger.posted`
5. **wire-notification** consumes `payment.succeeded` + `ledger.posted` → renders dynamic templates, delivers async via configured channel

### Data Flow — Saga Compensation (Failure Path)

If `payment.succeeded` fails (e.g., Stripe decline or network timeout):

1. Compensating transaction publishes `order.failed`
2. **wire-inventory** consumes `order.failed` → releases reserved stock, returns inventory to pool
3. If payment was partially processed → initiate `payment.refunded` via Stripe API
4. **wire-ledger** records reversal entries (debit/credit pair) for every compensation
5. **wire-notification** sends order-cancelled notification with reason

---

## Services

| Service | Port | Purpose | Database Schema Topics |
|---------|------|---------|----------------------|
| **wire-order** | 8081 | Order lifecycle management, cart state machines, distributed checkout sessions | `orders`, `order_items`, `checkout_sessions`, `outbox_events` |
| **wire-inventory** | 8082 | Stock allocation, Redlock/Pessimistic locks, reservation timeouts | `products`, `stock_allocations`, `reservations`, `outbox_events` |
| **wire-payment** | 8083 | Stripe/mock provider integrations, webhook ingestion, idempotency validation | `payments`, `payment_attempts`, `webhooks`, `idempotency_tokens`, `outbox_events` |
| **wire-notification** | 8084 | Async delivery, DLQ routing, dynamic templating | `notifications`, `notification_logs`, `dead_letter_queue`, `templates` |
| **wire-ledger** | 8085 | Immutable double-entry audit trail for all transactions | `ledger_entries`, `journal`, `reconciliation_audit`, `outbox_events` |

---

## Key Patterns

### Transactional Outbox Pattern
Every service writes domain events to a per-service `outbox_events` table within the same database transaction as the business state change. A polling-based publisher (no CDC dependency for simplicity) batches these events and publishes them to Kafka. This guarantees **exactly-once delivery semantics** — an event is only published after the corresponding DB write has committed.

### Saga Orchestration
A lightweight in-process Saga coordinator in `wire-order` manages the distributed checkout flow:
- **Compensation Actions:** Each step registers a compensation handler (release inventory, initiate refund)
- **State Machine:** Tracks saga progress per `order_id`; failures trigger backward compensation sequence
- **Timeouts:** Reservation timeout triggers automatic `inventory.released` event

### Idempotency & Concurrency
- **webhook/payment endpoints** enforce unique idempotency keys via Redis-set + atomic check
- **stock allocation** uses PostgreSQL row-level locking (`SELECT ... FOR UPDATE`) with Redlock as secondary safety net for high-contention inventory updates
- **Redis atomics** for flash-sale style inventory decrement with fallback to pessimistic DB lock

### Fault Tolerance
- **Circuit Breakers** (using `gobreaker`) on all outbound HTTP calls (Stripe, notification channels)
- **Exponential Backoff with Jitter** on Kafka producer retries and consumer poll failures
- **Dead Letter Queues (DLQ)**: Failed events land in service-specific DLQ topics after N retries, viewable via wire-notification's `/dlq` endpoint

### Distributed Locking
- **Redis Redlock** for cross-service inventory reservation coordination
- **PostgreSQL `FOR UPDATE`** as primary lock for stock decrement operations
- **Lock timeout with auto-release** prevents deadlocks even under service failures

---

## Project Structure

```
wirepay-orchestrator/
├── cmd/
│   ├── wire-order/           # Order lifecycle, cart state machine
│   ├── wire-inventory/       # Stock allocation, distributed locking
│   ├── wire-payment/         # Stripe/mock provider, webhooks
│   ├── wire-notification/    # Async delivery, templating, DLQ
│   └── wire-ledger/          # Double-entry audit trail
├── pkg/
│   ├── shared/
│   │   ├── config/           # Shared configuration loader (env + defaults)
│   │   ├── kafka/            # Kafka producer/consumer helpers
│   │   ├── postgres/         # PostgreSQL connection pooling, migrations helper
│   │   ├── redis/            # Redis client + Redlock implementation
│   │   ├── observability/    # OpenTelemetry tracing/metrics/logging setup
│   │   ├── middleware/       # HTTP middleware (auth, request ID, tracing)
│   │   └── outbox/           # Transactional outbox publisher (polling-based)
│   ├── types/                # Shared domain types and event contracts
│   └── httpserver/           # Common HTTP server setup with health/readiness
├── deploy/
│   ├── docker-compose.yml    # Full local dev stack (services + Kafka + PG + Redis)
│   └── Makefile              # Service build/run targets per component
├── k8s-manifests/            # Production Kubernetes manifests
│   ├── base/                 # Shared Kustomize base (ConfigMaps, Secrets, namespaces)
│   ├── wire-order/           # Deployment, Service, HPA, PDB
│   ├── wire-inventory/       # ... per service
│   ├── wire-payment/         # ...
│   ├── wire-notification/    # ...
│   └── wire-ledger/          # ...
├── scripts/                  # Setup, DB init, destroy helpers
├── load-tests/               # k6 + Toxiproxy + Chaos Mesh configurations
│   ├── k6/                   # Flash sale concurrency simulation
│   └── chaos/                # Network partition tests
├── .github/workflows/        # CI: lint, test, build per service
├── Makefile                  # Top-level orchestration
├── go.mod / go.work          # Go modules workspace
└── README.md                 # You are here
```

---

## Prerequisites

- **Go** 1.22+ (with module caching)
- **Docker** & **Docker Compose** v2.20+ (for local development stack)
- **kubectl** v1.28+ (for Kubernetes deployments)
- **k6** 0.45+ (for load testing)
- **Go Tools**: `golangci-lint`, `goimports`

---

## Quick Start (Local Dev)

### 1. Start the Full Stack

```bash
make docker-compose-up
```

This brings up:
- PostgreSQL instances (one per service, all on port range 54320+)
- Kafka (KRaft mode, single broker)
- Redis Cluster (3 nodes for production parity in dev)
- All 5 WPO services with auto-reload
- OpenTelemetry Collector + Jaeger/Zipkin

### 2. Initialize Service Databases

```bash
make init-dbs
```

Runs PostgreSQL migrations for each service's schema automatically.

### 3. Run Tests

```bash
# All services
make test-all

# Single service
make test SERVICE=wire-order
```

### 4. Build & Tag

```bash
make build-all          # Build all service binaries
make tag-all            # Tag with local image (localhost:5000/wpo/wire-{service}:local)
```

---

## Production Deployment

### Kubernetes Manifests

```bash
# Deploy to cluster (k8s manifests include HPA, PDB, ConfigMaps/Secrets)
kubectl apply -f k8s-manifests/base/namespace.yaml
kubectl apply -k k8s-manifests/wire-order
kubectl apply -k k8s-manifests/wire-inventory
kubectl apply -k k8s-manifests/wire-payment
kubectl apply -k k8s-manifests/wire-notification
kubectl apply -k k8s-manifests/wire-ledger

# Scale horizontally with HPA (CPU/memory based)
# Wire-Inventory is the critical path — configured for aggressive HPA
kubectl scale deployment wire-inventory -n wpo --replicas=3
```

### Environment Variables

All services support the same core configuration via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `WPO_SERVICE_NAME` | Service identifier for tracing/metrics | (required) |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` |
| `KAFKA_CONSUMER_GROUP` | Kafka consumer group ID | `{service_name}-group` |
| `DATABASE_URL` | PostgreSQL connection string | `postgres://user:pass@host:5432/{db}` |
| `REDIS_URL` | Redis connection string | `redis://localhost:6379` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP collector endpoint | `http://otel-collector:4317` |
| `STRIPE_MOCK_MODE` | Run payments in mock mode (no Stripe) | `true` |
| `CIRCUIT_BREAKER_MAX_FAILURES` | Circuit breaker trip threshold | `5` |

---

## Load Testing

### Flash Sale Scenario

Simulates 10,000 concurrent checkout requests against a single zero-stock product:

```bash
make load-test-flash-sale
# Runs: k6 run load-tests/k6/flash-sale.js --vus=500 --duration=60s
```

### What to Verify

- ✅ Zero inventory race conditions (no overselling)
- ✅ All failed orders compensate inventory reservation
- ✅ No Kafka consumer group rebalances under load
- ✅ Circuit breakers trip and recover within expected SLA
- ✅ DLQ events are captured for all failure cases

---

## Chaos Engineering

### Toxiproxy Configurations

```bash
# Simulate network partition between wire-payment and Kafka
make chaos-partition-payment-kafka

# Apply timeout injection on wire-order → wire-inventory calls
make chaos-timeout-order-inventory
```

### Chaos Mesh (Kubernetes)

Apply a 30-second partition between wire-payment and its database:

```bash
kubectl apply -f load-tests/chaos/payment-network-partition.yaml
# After 30s, verify no data loss in wire-payment's transactions
kubectl delete -f load-tests/chaos/payment-network-partition.yaml
```

---

## Observability

### Metrics (Prometheus)

Each service exposes `/metrics` with:
- `wpo_requests_total{service,method,path,status}` — HTTP request counters
- `wpo_kafka_messages_processed_total{service,event_type}` — Kafka event counters
- `wpo_inventory_stock_remaining{product_id}` — Real-time stock levels
- `wpo_payment_latency_seconds{provider,status}` — Payment processing latency histogram
- `wpo_ledger_entries_total` — Accumulated ledger entries

### Distributed Tracing (Jaeger)

- Full OpenTelemetry instrumentation with W3C trace context propagation through Kafka headers
- Sample: 10% sampling in dev, 1% in prod (configurable per service)
- Trace correlation across all 5 services via `order_id` span tags

### Structured Logging

- All logs emitted as JSON with structured fields (`trace_id`, `span_id`, `service`, `order_id`)
- Log level configurable per service via environment variable
- Default: INFO in dev, WARN in prod

---

## Development Workflow

### Running a Single Service Locally

```bash
cd cmd/wire-order && go run ./main.go
# Or with hot reload (if configured):
make run SERVICE=wire-order
```

### Adding a New Event Type

1. Define the event struct in `pkg/types/events.go`
2. Add the event constant to the consumer groups in each service's config
3. Implement the handler in the target service's `handlers/` directory
4. Run migration if new database schema needed
5. Update Kafka topic configuration in `deploy/docker-compose.yml`

### Database Migrations

Each service manages its own PostgreSQL migrations via embedded SQL files in `migrations/{service}/`:
- Use 001, 002, 003 style ordering (timestamp-based naming if needed)
- Run via `make migrate SERVICE=wire-order`

---

## Contributing

This is a portfolio project by [Thomas Wire](https://thomaswire.com). Contributions welcome via pull requests for educational purposes. All business logic and architectural decisions are production-grade implementations meant to demonstrate enterprise distributed systems competency.

---

## License

MIT © 2024 Thomas Wire
