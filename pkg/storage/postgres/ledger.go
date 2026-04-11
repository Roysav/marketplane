// Package postgres provides a PostgreSQL LedgerStorage implementation.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/roysav/marketplane/pkg/storage"
)

// LedgerStorage implements storage.LedgerStorage using PostgreSQL.
// It uses PostgreSQL-specific features for proper transaction isolation:
// - Advisory locks for serializing writes per tradespace+currency
// - SELECT FOR UPDATE for row-level locking
// - SERIALIZABLE isolation for the balance check
type LedgerStorage struct {
	db *sql.DB
}

// NewLedgerStorage creates a new PostgreSQL ledger storage.
func NewLedgerStorage(ctx context.Context, dsn string) (*LedgerStorage, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Run migrations (shared with RowStorage)
	if err := Migrate(ctx, db); err != nil {
		return nil, fmt.Errorf("migration failed: %w", err)
	}

	return &LedgerStorage{db: db}, nil
}

// Append atomically checks balance and inserts a ledger entry.
// Uses PostgreSQL advisory locks to serialize writes per tradespace+currency,
// preventing race conditions that could result in negative balances.
func (s *LedgerStorage) Append(ctx context.Context, e *storage.LedgerEntry) error {
	// Validate amount format
	amount, err := decimal.NewFromString(e.Amount)
	if err != nil {
		return fmt.Errorf("invalid amount: %w", err)
	}

	// Generate UUID server-side
	e.ID = uuid.New().String()
	e.CreatedAt = time.Now().UTC()

	// Use SERIALIZABLE isolation for strict consistency
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Acquire advisory lock for this tradespace+currency combination.
	// This serializes all writes for the same tradespace+currency,
	// preventing concurrent transactions from seeing stale balances.
	lockKey := advisoryLockKey(e.Tradespace, e.Currency)
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey); err != nil {
		return fmt.Errorf("failed to acquire advisory lock: %w", err)
	}

	// Check one-to-one constraint: target can only have one allocation.
	// Use SELECT FOR UPDATE to lock any existing row.
	var existingID string
	err = tx.QueryRowContext(ctx, `
		SELECT id FROM ledger
		WHERE tradespace = $1 AND target_type = $2 AND target_name = $3
		FOR UPDATE
	`, e.Tradespace, e.TargetType, e.TargetName).Scan(&existingID)
	if err == nil {
		return storage.ErrAlreadyAllocated
	}
	if err != sql.ErrNoRows {
		return fmt.Errorf("failed to check existing allocation: %w", err)
	}

	// For negative amounts (spending), check balance constraint.
	// The advisory lock ensures serialization, so we can safely read the balance.
	if amount.IsNegative() {
		var balanceStr sql.NullString
		err = tx.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(amount), 0)::TEXT
			FROM ledger
			WHERE tradespace = $1 AND currency = $2
		`, e.Tradespace, e.Currency).Scan(&balanceStr)
		if err != nil {
			return fmt.Errorf("failed to get balance: %w", err)
		}

		balance := decimal.Zero
		if balanceStr.Valid && balanceStr.String != "" {
			balance, _ = decimal.NewFromString(balanceStr.String)
		}

		if balance.Add(amount).IsNegative() {
			return storage.ErrInsufficientBalance
		}
	}

	// Insert the entry
	_, err = tx.ExecContext(ctx, `
		INSERT INTO ledger (id, tradespace, currency, amount, allocation_name, target_type, target_name, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, e.ID, e.Tradespace, e.Currency, e.Amount, e.AllocationName, e.TargetType, e.TargetName, e.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to insert ledger entry: %w", err)
	}

	return tx.Commit()
}

// Balance returns SUM(amount) for tradespace+currency.
func (s *LedgerStorage) Balance(ctx context.Context, tradespace, currency string) (string, error) {
	var balanceStr sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(amount), 0)::TEXT
		FROM ledger
		WHERE tradespace = $1 AND currency = $2
	`, tradespace, currency).Scan(&balanceStr)
	if err != nil {
		return "0", fmt.Errorf("failed to get balance: %w", err)
	}

	if !balanceStr.Valid || balanceStr.String == "" {
		return "0", nil
	}

	// Parse and reformat for consistent representation
	balance, err := decimal.NewFromString(balanceStr.String)
	if err != nil {
		return "0", nil
	}

	return balance.String(), nil
}

// GetByTarget returns entry for a target record.
func (s *LedgerStorage) GetByTarget(ctx context.Context, tradespace, targetType, targetName string) (*storage.LedgerEntry, error) {
	entry := &storage.LedgerEntry{}
	var amount string

	err := s.db.QueryRowContext(ctx, `
		SELECT id, tradespace, currency, amount::TEXT, allocation_name, target_type, target_name, created_at
		FROM ledger
		WHERE tradespace = $1 AND target_type = $2 AND target_name = $3
	`, tradespace, targetType, targetName).Scan(
		&entry.ID, &entry.Tradespace, &entry.Currency, &amount,
		&entry.AllocationName, &entry.TargetType, &entry.TargetName, &entry.CreatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get ledger entry: %w", err)
	}

	entry.Amount = amount
	return entry, nil
}

// List returns all entries for a tradespace.
func (s *LedgerStorage) List(ctx context.Context, tradespace string) ([]*storage.LedgerEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tradespace, currency, amount::TEXT, allocation_name, target_type, target_name, created_at
		FROM ledger
		WHERE tradespace = $1
		ORDER BY created_at ASC
	`, tradespace)
	if err != nil {
		return nil, fmt.Errorf("failed to list ledger entries: %w", err)
	}
	defer rows.Close()

	var entries []*storage.LedgerEntry
	for rows.Next() {
		entry := &storage.LedgerEntry{}
		var amount string

		err := rows.Scan(
			&entry.ID, &entry.Tradespace, &entry.Currency, &amount,
			&entry.AllocationName, &entry.TargetType, &entry.TargetName, &entry.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan ledger entry: %w", err)
		}

		entry.Amount = amount
		entries = append(entries, entry)
	}

	return entries, rows.Err()
}

// Close releases resources.
func (s *LedgerStorage) Close() error {
	return s.db.Close()
}

// advisoryLockKey generates a consistent int64 key for pg_advisory_lock
// based on tradespace and currency.
func advisoryLockKey(tradespace, currency string) int64 {
	h := fnv.New64a()
	h.Write([]byte(tradespace))
	h.Write([]byte{0})
	h.Write([]byte(currency))
	return int64(h.Sum64())
}
