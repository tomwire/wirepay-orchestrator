// Package repository provides PostgreSQL data access for wire-ledger service.
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// Account represents a ledger account.
type Account struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Type      string    `json:"type"` // "ASSET", "LIABILITY", etc.
	Balance   int64     `json:"balance_cents"`
	Currency  string    `json:"currency"`
	Metadata  *string   `json:"metadata,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// JournalEntry represents a double-entry bookkeeping line.
type JournalEntry struct {
	ID          string    `json:"id"`
	TransactionID string  `json:"transaction_id"`
	AccountID   string    `json:"account_id"`
	Description *string   `json:"description,omitempty"`
	CreditAmount int64    `json:"credit_cents"`
	DebitAmount  int64    `json:"debit_cents"`
	Metadata    *string   `json:"metadata,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// Transaction represents a group of journal entries.
type Transaction struct {
	ID          string     `json:"id"`
	Status      string     `json:"status"` // "PENDING", "POSTED"
	ReferenceID *string    `json:"reference_id,omitempty"`
	Metadata    *string    `json:"metadata,omitempty"`
	PostedAt    *time.Time `json:"posted_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
}

var (
	// ErrNotFound is returned when a record doesn't exist.
	ErrNotFound = errors.New("repository: not found")
)

// LedgerRepo handles PostgreSQL interactions for the general ledger.
type LedgerRepo struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// NewLedgerRepo creates a new repository with the given connection pool.
func NewLedgerRepo(pool *pgxpool.Pool, logger *zap.Logger) *LedgerRepo {
	return &LedgerRepo{pool: pool, logger: logger}
}

// CreateAccount inserts a new ledger account and returns it with generated ID.
func (r *LedgerRepo) CreateAccount(ctx context.Context, acct *Account) error {
	if acct.ID == "" {
		acctID, err := uuid.NewV4()
		if err != nil {
			return fmt.Errorf("generate account UUID: %w", err)
		}
		acct.ID = acctID.String()
	}

	now := time.Now().UTC()
	const sql = `
		INSERT INTO accounts (id, name, type, balance, currency, metadata, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id, name, type, balance, currency, metadata, created_at, updated_at
	`

	err := r.pool.QueryRow(ctx, sql, acct.ID, acct.Name, acct.Type, acct.Balance, acct.Currency,
		acct.Metadata, now, now).Scan(
		&acct.ID, &acct.Name, &acct.Type, &acct.Balance, &acct.Currency,
		&acct.Metadata, &acct.CreatedAt, &acct.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("create account: %w", err)
	}

	return nil
}

// GetAccount retrieves an account by ID.
func (r *LedgerRepo) GetAccount(ctx context.Context, id string) (*Account, error) {
	var acct Account

	err := r.pool.QueryRow(ctx, `
		SELECT id, name, type, balance, currency, metadata, created_at, updated_at
		FROM accounts WHERE id = $1
	`, id).Scan(
		&acct.ID, &acct.Name, &acct.Type, &acct.Balance, &acct.Currency,
		&acct.Metadata, &acct.CreatedAt, &acct.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get account %s: %w", id, err)
	}

	return &acct, nil
}

// InsertJournalEntry adds a journal entry to the ledger.
func (r *LedgerRepo) InsertJournalEntry(ctx context.Context, entry *JournalEntry) error {
	if entry.ID == "" {
		entryID, err := uuid.NewV4()
		if err != nil {
			return fmt.Errorf("generate journal entry UUID: %w", err)
		}
		entry.ID = entryID.String()
	}

	now := time.Now().UTC()
	const sql = `
		INSERT INTO journal_entries (id, transaction_id, account_id, description, credit_cents, debit_cents, metadata, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	_, err := r.pool.Exec(ctx, sql, entry.ID, entry.TransactionID, entry.AccountID,
		entry.Description, entry.CreditAmount, entry.DebitAmount, entry.Metadata, now)
	if err != nil {
		return fmt.Errorf("insert journal entry: %w", err)
	}

	return nil
}

// CreateTransaction inserts a new transaction record.
func (r *LedgerRepo) CreateTransaction(ctx context.Context, tx *Transaction) error {
	if tx.ID == "" {
		txID, err := uuid.NewV4()
		if err != nil {
			return fmt.Errorf("generate transaction UUID: %w", err)
		}
		tx.ID = txID.String()
	}

	now := time.Now().UTC()
	const sql = `
		INSERT INTO transactions (id, status, reference_id, metadata, created_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, status, reference_id, metadata, created_at
	`

	err := r.pool.QueryRow(ctx, sql, tx.ID, tx.Status, tx.ReferenceID, tx.Metadata, now).Scan(
		&tx.ID, &tx.Status, &tx.ReferenceID, &tx.Metadata, &tx.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("create transaction: %w", err)
	}

	return nil
}

// PostTransaction marks a transaction as posted.
func (r *LedgerRepo) PostTransaction(ctx context.Context, id string) error {
	now := time.Now().UTC()
	const sql = `UPDATE transactions SET status = $1, posted_at = $2 WHERE id = $3`

	_, err := r.pool.Exec(ctx, sql, "POSTED", &now, id)
	if err != nil {
		return fmt.Errorf("post transaction %s: %w", id, err)
	}

	return nil
}

// GetTransaction retrieves a transaction by ID.
func (r *LedgerRepo) GetTransaction(ctx context.Context, id string) (*Transaction, error) {
	var tx Transaction
	var postedAt time.Time

	err := r.pool.QueryRow(ctx, `
		SELECT id, status, reference_id, metadata, posted_at, created_at
		FROM transactions WHERE id = $1
	`, id).Scan(
		&tx.ID, &tx.Status, &tx.ReferenceID, &tx.Metadata,
		&postedAt, &tx.CreatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get transaction %s: %w", id, err)
	}

	tx.PostedAt = &postedAt
	return &tx, nil
}

// GetJournalEntriesByTransaction retrieves all journal entries for a transaction.
func (r *LedgerRepo) GetJournalEntriesByTransaction(ctx context.Context, transactionID string) ([]*JournalEntry, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, transaction_id, account_id, description, credit_cents, debit_cents, metadata, created_at
		FROM journal_entries WHERE transaction_id = $1 ORDER BY created_at ASC
	`, transactionID)
	if err != nil {
		return nil, fmt.Errorf("query journal entries: %w", err)
	}
	defer rows.Close()

	var entries []*JournalEntry
	for rows.Next() {
		var e JournalEntry
		if err := rows.Scan(&e.ID, &e.TransactionID, &e.AccountID, &e.Description,
			&e.CreditAmount, &e.DebitAmount, &e.Metadata, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan journal entry: %w", err)
		}
		entries = append(entries, &e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate journal entries: %w", err)
	}

	return entries, nil
}

// GetAccountBalance retrieves the current balance for an account.
func (r *LedgerRepo) GetAccountBalance(ctx context.Context, accountID string) (int64, error) {
	var balance int64

	err := r.pool.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1`, accountID).Scan(&balance)
	if err != nil {
		return 0, fmt.Errorf("get account balance: %w", err)
	}

	return balance, nil
}
