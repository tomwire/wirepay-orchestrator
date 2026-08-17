-- Migration: 001_create_payments.sql
-- Creates the core tables for the wire-payment service: payments, payment attempts,
-- webhook events, and the transactional outbox.

BEGIN;

-- ============================================================
-- payments table
-- Stores the primary payment entity with its lifecycle state.
-- Each row represents one idempotently-createable payment intent.
-- ============================================================
CREATE TABLE IF NOT EXISTS payments (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id            UUID         NOT NULL,
    amount              DECIMAL(12,2) NOT NULL CHECK (amount > 0),
    currency            VARCHAR(3)   NOT NULL DEFAULT 'USD',
    status              VARCHAR(20)  NOT NULL DEFAULT 'PENDING'
                            CHECK (status IN ('PENDING','PROCESSING','COMPLETED','FAILED','REFUNDED','CANCELLED')),
    provider_payment_id VARCHAR(100),         -- Stripe PaymentIntent ID or mock equivalent
    idempotency_key     VARCHAR(100) UNIQUE,  -- Dedup key for retry-safe create operations
    metadata            JSONB            DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments (order_id);
CREATE INDEX IF NOT EXISTS idx_payments_status ON payments (status);
CREATE INDEX IF NOT EXISTS idx_payments_provider_id ON payments (provider_payment_id) WHERE provider_payment_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_payments_created_at ON payments (created_at DESC);

-- ============================================================
-- payment_attempts table
-- Audit trail of every attempt to process a payment with the provider.
-- Useful for debugging flaky external APIs and understanding retry history.
-- ============================================================
CREATE TABLE IF NOT EXISTS payment_attempts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id      UUID         NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    attempt_number  INT          NOT NULL DEFAULT 1,
    provider_response JSONB      NOT NULL DEFAULT '{}',
    status          VARCHAR(20)  NOT NULL,
    error_message   TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_payment_attempts_payment_id ON payment_attempts (payment_id);
CREATE INDEX IF NOT EXISTS idx_payment_attempts_sequence ON payment_attempts (payment_id, attempt_number DESC);

-- ============================================================
-- webhooks table
-- Persistent log of every webhook event received from the provider.
-- Enables replay and audit without relying solely on Kafka.
-- ============================================================
CREATE TABLE IF NOT EXISTS webhooks (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type      VARCHAR(50)  NOT NULL,
    event_data      JSONB        NOT NULL,
    processed       BOOLEAN      NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_webhooks_event_type ON webhooks (event_type);
CREATE INDEX IF NOT EXISTS idx_webhooks_processed ON webhooks (processed) WHERE processed = false;

-- ============================================================
-- outbox_events table
-- Transactional outbox: events are written alongside business
-- state changes, then asynchronously published to Kafka.
-- This guarantees exactly-once delivery semantics with no loss.
-- ============================================================
CREATE TABLE IF NOT EXISTS outbox_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type      VARCHAR(128) NOT NULL,
    service_name    VARCHAR(64)  NOT NULL DEFAULT 'wire-payment',
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

-- ============================================================
-- Helper: trigger to auto-update updated_at on payments table.
-- ============================================================
CREATE OR REPLACE FUNCTION update_payments_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_payments_updated_at ON payments;
CREATE TRIGGER trg_payments_updated_at
    BEFORE UPDATE ON payments
    FOR EACH ROW EXECUTE FUNCTION update_payments_updated_at();

COMMIT;
