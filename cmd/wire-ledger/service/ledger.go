// Package service implements the business logic for wire-ledger.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/thomaswire/wpo/cmd/wire-ledger/repository"
)

var tracer = otel.Tracer("github.com/thomaswire/wpo/cmd/wire-ledger/service")

// LedgerService handles all ledger business logic and double-entry bookkeeping.
type LedgerService struct {
	repo    *repository.LedgerRepo
	logger  *zap.Logger
	outbox  interface{} // outbox publisher placeholder
}

// NewLedgerService creates a new LedgerService with the given dependencies.
func NewLedgerService(repo *repository.LedgerRepo, logger *zap.Logger, outbox interface{}) *LedgerService {
	return &LedgerService{repo: repo, logger: logger, outbox: outbox}
}

// CreateAccountInput holds the data needed to create a new ledger account.
type CreateAccountInput struct {
	Name     string          `json:"name"`
	Type     string          `json:"type"` // ASSET, LIABILITY, EQUITY, REVENUE, EXPENSE
	Currency string          `json:"currency"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// AccountResponse is the structured response returned after account operations.
type AccountResponse struct {
	ID       string    `json:"id"`
	Name     string    `json:"name"`
	Type     string    `json:"type"`
	Currency string    `json:"currency"`
	CreatedAt time.Time `json:"created_at"`
}

// CreateAccount creates a new ledger account and persists it.
func (s *LedgerService) CreateAccount(ctx context.Context, input CreateAccountInput) (*AccountResponse, error) {
	ctx, span := tracer.Start(ctx, "LedgerService.CreateAccount")
	defer span.End()

	acct := &repository.Account{
		Name:     input.Name,
		Type:     input.Type,
		Currency: input.Currency,
		Balance:  0,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	if err := s.repo.CreateAccount(ctx, acct); err != nil {
		return nil, fmt.Errorf("service: create account: %w", err)
	}

	s.logger.Info("account created", zap.String("id", acct.ID))

	return &AccountResponse{
		ID:        acct.ID,
		Name:      acct.Name,
		Type:      acct.Type,
		Currency:  acct.Currency,
		CreatedAt: acct.CreatedAt,
	}, nil
}

// GetAccount retrieves a ledger account by ID.
func (s *LedgerService) GetAccount(ctx context.Context, accountID string) (*repository.Account, error) {
	ctx, span := tracer.Start(ctx, "LedgerService.GetAccount")
	defer span.End()

	acct, err := s.repo.GetAccount(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("service: get account %s: %w", accountID, err)
	}

	return acct, nil
}

// CreateTransactionInput holds the data needed to create a new transaction.
type CreateTransactionInput struct {
	Status      string
	ReferenceID *string
	Metadata    json.RawMessage
}

// TransactionResponse is the structured response returned after transaction operations.
type TransactionResponse struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// CreateTransaction creates a new ledger transaction and persists it.
func (s *LedgerService) CreateTransaction(ctx context.Context, input CreateTransactionInput) (*TransactionResponse, error) {
	ctx, span := tracer.Start(ctx, "LedgerService.CreateTransaction")
	defer span.End()

	tx := &repository.Transaction{
		Status:    input.Status,
		CreatedAt: time.Now().UTC(),
	}

	if err := s.repo.CreateTransaction(ctx, tx); err != nil {
		return nil, fmt.Errorf("service: create transaction: %w", err)
	}

	s.logger.Info("transaction created", zap.String("id", tx.ID))

	return &TransactionResponse{
		ID:        tx.ID,
		Status:    tx.Status,
		CreatedAt: tx.CreatedAt,
	}, nil
}

// PostTransaction marks a pending transaction as posted.
func (s *LedgerService) PostTransaction(ctx context.Context, transactionID string) error {
	ctx, span := tracer.Start(ctx, "LedgerService.PostTransaction")
	defer span.End()

	if err := s.repo.PostTransaction(ctx, transactionID); err != nil {
		return fmt.Errorf("service: post transaction %s: %w", transactionID, err)
	}

	s.logger.Info("transaction posted", zap.String("id", transactionID))
	return nil
}

// GetTransaction retrieves a transaction by ID.
func (s *LedgerService) GetTransaction(ctx context.Context, transactionID string) (*repository.Transaction, error) {
	ctx, span := tracer.Start(ctx, "LedgerService.GetTransaction")
	defer span.End()

	tx, err := s.repo.GetTransaction(ctx, transactionID)
	if err != nil {
		return nil, fmt.Errorf("service: get transaction %s: %w", transactionID, err)
	}

	return tx, nil
}

// CreateJournalEntry creates a journal entry for the given transaction.
func (s *LedgerService) CreateJournalEntry(ctx context.Context, transactionID string, accountID string, creditCents int64, debitCents int64) (*repository.JournalEntry, error) {
	ctx, span := tracer.Start(ctx, "LedgerService.CreateJournalEntry")
	defer span.End()

	entry := &repository.JournalEntry{
		TransactionID: transactionID,
		AccountID:     accountID,
		CreditAmount:  creditCents,
		DebitAmount:   debitCents,
		CreatedAt:     time.Now().UTC(),
	}

	if err := s.repo.InsertJournalEntry(ctx, entry); err != nil {
		return nil, fmt.Errorf("service: create journal entry: %w", err)
	}

	s.logger.Info("journal entry created", zap.String("id", entry.ID))
	return entry, nil
}

// GetJournalEntriesByTransaction retrieves all journal entries for a transaction.
func (s *LedgerService) GetJournalEntriesByTransaction(ctx context.Context, transactionID string) ([]*repository.JournalEntry, error) {
	ctx, span := tracer.Start(ctx, "LedgerService.GetJournalEntriesByTransaction")
	defer span.End()

	entries, err := s.repo.GetJournalEntriesByTransaction(ctx, transactionID)
	if err != nil {
		return nil, fmt.Errorf("service: get journal entries: %w", err)
	}

	return entries, nil
}

// GetAccountBalance retrieves the current balance for an account.
func (s *LedgerService) GetAccountBalance(ctx context.Context, accountID string) (int64, error) {
	ctx, span := tracer.Start(ctx, "LedgerService.GetAccountBalance")
	defer span.End()

	balance, err := s.repo.GetAccountBalance(ctx, accountID)
	if err != nil {
		return 0, fmt.Errorf("service: get balance for %s: %w", accountID, err)
	}

	return balance, nil
}

// Ensure uuid import is used.
func _useUUID() {
	_, _ = uuid.NewV4()
}
