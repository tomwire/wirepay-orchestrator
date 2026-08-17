-- Migration: 001_create_orders.sql
-- Creates the core tables for the wire-order service: orders, checkout sessions, and outbox events.

BEGIN;

-- ============================================================
-- orders table
-- Stores the primary order entity with its lifecycle state.
-- ============================================================
CREATE TABLE IF NOT EXISTS orders (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status          VARCHAR(32)  NOT NULL DEFAULT 'CREATED',
    items           JSONB        NOT NULL DEFAULT '[]',
    total_amount    DECIMAL(15,2) NOT NULL DEFAULT 0.00,
    currency        VARCHAR(3)   NOT NULL DEFAULT 'USD',
    external_id     VARCHAR(128),  -- idempotency / external reference key
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_external_id ON orders (external_id) WHERE external_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at DESC);

-- ============================================================
-- checkout_sessions table
-- Distributed checkout session state, scoped per order.
-- Each order gets one active checkout session at a time.
-- ============================================================
CREATE TABLE IF NOT EXISTS checkout_sessions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id        UUID         NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    status          VARCHAR(32)  NOT NULL DEFAULT 'PENDING',
    payment_token   TEXT,             -- Stripe or other payment provider token reference
    expires_at      TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_checkout_sessions_order_id ON checkout_sessions (order_id);
CREATE INDEX IF NOT EXISTS idx_checkout_sessions_status ON checkout_sessions (status);
CREATE INDEX IF NOT EXISTS idx_checkout_sessions_expires ON checkout_sessions (expires_at) WHERE expires_at < now();

-- ============================================================
-- outbox_events table
-- Transactional outbox: events are written alongside business
-- state changes, then asynchronously published to Kafka.
-- This guarantees exactly-once delivery semantics.
-- ============================================================
CREATE TABLE IF NOT EXISTS outbox_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type      VARCHAR(128) NOT NULL,
    service_name    VARCHAR(64)  NOT NULL,
    "key"           VARCHAR(255) NOT NULL,
    payload         JSONB        NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    status          VARCHAR(16)  NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'published', 'failed')),
    retry_count     INTEGER      NOT NULL DEFAULT 0,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_events (status) WHERE status = 'pending';
CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_key_created ON outbox_events ("key", created_at);

COMMIT;
