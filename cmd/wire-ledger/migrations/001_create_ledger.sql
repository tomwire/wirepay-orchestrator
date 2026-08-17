-- Migration: 001_create_ledger.sql
-- Wire Ledger Service schema — double-entry bookkeeping for financial transactions.
-- Each transaction has at least two entries (debit + credit) that sum to zero.

BEGIN;

-- ============================================================
-- accounts: the chart of accounts with running balances
-- ============================================================
CREATE TABLE IF NOT EXISTS accounts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code            VARCHAR(20)  UNIQUE NOT NULL,     -- e.g. '1100' for cash, '4100' for revenue
    name            TEXT         NOT NULL,
    type            VARCHAR(20)  NOT NULL DEFAULT 'asset'
                        CHECK (type IN ('asset', 'liability', 'equity', 'revenue', 'expense')),
    currency        VARCHAR(3)   NOT NULL DEFAULT 'USD',
    balance_cents   BIGINT       NOT NULL DEFAULT 0,  -- integer cents to avoid float issues
    metadata        JSONB        DEFAULT '{}',
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_accounts_type ON accounts(type);
CREATE INDEX idx_accounts_code ON accounts(code);

-- ============================================================
-- journal_entries: immutable records of financial movements
-- Every transaction is split into entries; the sum of debits equals the sum of credits.
-- ============================================================
CREATE TABLE IF NOT EXISTS journal_entries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id  UUID         NOT NULL,            -- groups related entries
    account_id      UUID         NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    entry_type      VARCHAR(10)  NOT NULL CHECK (entry_type IN ('debit', 'credit')),
    amount_cents    BIGINT       NOT NULL CHECK (amount_cents > 0),
    description     TEXT,
    reference_id    UUID,                             -- links back to source entity (payment, etc.)
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_journal_entries_transaction ON journal_entries(transaction_id);
CREATE INDEX idx_journal_entries_account ON journal_entries(account_id);
CREATE INDEX idx_journal_entries_reference ON journal_entries(reference_id) WHERE reference_id IS NOT NULL;

-- ============================================================
-- transactions: high-level financial operations that span multiple entries
-- A single transaction (e.g., a payment) produces N debit + M credit entries.
-- ============================================================
CREATE TABLE IF NOT EXISTS transactions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type            VARCHAR(30)  NOT NULL,             -- e.g. 'payment_received', 'refund', 'transfer'
    status          VARCHAR(20)  NOT NULL DEFAULT 'posted'
                        CHECK (status IN ('draft', 'posted', 'reversed')),
    currency        VARCHAR(3)   NOT NULL,
    total_debits_cents BIGINT   NOT NULL CHECK (total_debits_cents = total_credits_cents),
    total_credits_cents BIGINT  NOT NULL,
    metadata        JSONB        DEFAULT '{}',
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    posted_at       TIMESTAMPTZ
);

CREATE INDEX idx_transactions_status ON transactions(status);
CREATE INDEX idx_transactions_type ON transactions(type);

-- ============================================================
-- outbox_events: transactional outbox (shared schema with other WPO services)
-- ============================================================
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

-- ============================================================
-- Triggers for updated_at
-- ============================================================
CREATE OR REPLACE FUNCTION trg_ledger_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_accounts_updated_at ON accounts;
CREATE TRIGGER trg_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW EXECUTE FUNCTION trg_ledger_updated_at();

-- ============================================================
-- Balance update trigger: when journal entries are inserted, update account balances.
-- This ensures consistency — we could also maintain balance in app logic, but a trigger
-- provides a second layer of safety for any direct DB writes.
-- ============================================================
CREATE OR REPLACE FUNCTION trg_update_account_balance() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.entry_type = 'debit' THEN
        UPDATE accounts SET balance_cents = balance_cents + NEW.amount_cents WHERE id = NEW.account_id;
    ELSIF NEW.entry_type = 'credit' THEN
        UPDATE accounts SET balance_cents = balance_cents - NEW.amount_cents WHERE id = NEW.account_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_journal_entries_balance ON journal_entries;
CREATE TRIGGER trg_journal_entries_balance
    AFTER INSERT ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION trg_update_account_balance();

COMMIT;
