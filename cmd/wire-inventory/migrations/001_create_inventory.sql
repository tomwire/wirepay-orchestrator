-- Wire Inventory Service Database Schema
-- Ensures stock integrity and reservation tracking for flash-sale scenarios.

BEGIN;

-- =============================================================================
-- products: current product catalog with live stock quantities
-- =============================================================================
CREATE TABLE IF NOT EXISTS products (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sku             VARCHAR(64)  UNIQUE NOT NULL,
    name            TEXT         NOT NULL,
    stock_quantity  BIGINT       NOT NULL DEFAULT 0,
    reserved_quantity BIGINT     NOT NULL DEFAULT 0,
    status          VARCHAR(20)  NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'discontinued')),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_products_status ON products(status);
CREATE INDEX idx_products_sku ON products(sku);

-- =============================================================================
-- reservations: stock allocations against an order that may expire
-- =============================================================================
CREATE TABLE IF NOT EXISTS reservations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id      UUID         NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    order_id        UUID         NOT NULL,
    quantity        BIGINT       NOT NULL CHECK (quantity > 0),
    status          VARCHAR(20)  NOT NULL DEFAULT 'PENDING'
                        CHECK (status IN ('PENDING', 'RELEASED', 'EXPIRED')),
    expires_at      TIMESTAMPTZ  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_reservations_product_id ON reservations(product_id);
CREATE INDEX idx_reservations_order_id   ON reservations(order_id);
CREATE INDEX idx_reservations_status     ON reservations(status);
CREATE INDEX idx_reservations_expires_at ON reservations(expires_at) WHERE status = 'PENDING';

-- =============================================================================
-- stock_allocations: immutable audit log of every stock change
-- =============================================================================
CREATE TYPE stock_change_type AS ENUM (
    'RESERVATION',   -- Stock decremented, reservation created
    'RELEASE',       -- Stock re-incremented, reservation released
    'EXPIRED'        -- Stock re-incremented, reservation expired (timeout)
);

CREATE TABLE IF NOT EXISTS stock_allocations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id      UUID         NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    change_amount   BIGINT       NOT NULL,          -- positive = +stock, negative = -stock
    change_type     stock_change_type NOT NULL,
    reservation_id  UUID,                             -- FK → reservations (nullable for manual adjustments)
    note            TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_stock_allocations_product ON stock_allocations(product_id);
CREATE INDEX idx_stock_allocations_created ON stock_allocations(created_at);

-- =============================================================================
-- outbox_events: transactional outbox — same schema used by other WPO services
-- Events are written here within the same DB transaction as state changes,
-- then asynchronously published to Kafka by a background poller.
-- =============================================================================
CREATE TABLE IF NOT EXISTS outbox_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type      VARCHAR(128) NOT NULL,
    service_name    VARCHAR(64)  NOT NULL,
    "key"           TEXT         NOT NULL DEFAULT '',
    payload         JSONB        NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    status          VARCHAR(20)  NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'published', 'failed')),
    retry_count     INT          NOT NULL DEFAULT 0,
    error_message   TEXT,

    UNIQUE (id)
);

CREATE INDEX idx_outbox_status ON outbox_events(status, created_at) WHERE status = 'pending';

-- =============================================================================
-- Updated-at trigger: auto-timestamp on product and reservation tables
-- =============================================================================
CREATE OR REPLACE FUNCTION trg_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_products_updated_at ON products;
CREATE TRIGGER trg_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION trg_updated_at();

DROP TRIGGER IF EXISTS trg_reservations_updated_at ON reservations;
CREATE TRIGGER trg_reservations_updated_at
    BEFORE UPDATE ON reservations
    FOR EACH ROW EXECUTE FUNCTION trg_reservations_updated_at();

COMMIT;
