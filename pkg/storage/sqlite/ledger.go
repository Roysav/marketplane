package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"time"

	"github.com/shopspring/decimal"

	"github.com/roysav/marketplane/pkg/storage"
)

// LedgerStorage implements storage.LedgerStorage using SQLite.
type LedgerStorage struct {
	db *sql.DB
}

// NewLedgerStorage creates a new SQLite ledger storage.
func NewLedgerStorage(ctx context.Context, dsn string) (*LedgerStorage, error) {
	// For in-memory databases, use shared cache to share across connections
	if dsn == ":memory:" {
		dsn = "file::memory:?cache=shared"
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return nil, err
	}
	if dsn != ":memory:" && dsn != "file::memory:?cache=shared" {
		if _, err := db.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
			return nil, err
		}
	}

	s := &LedgerStorage{db: db}
	if err := s.migrate(ctx); err != nil {
		return nil, fmt.Errorf("migration failed: %w", err)
	}

	return s, nil
}

func (s *LedgerStorage) migrate(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS ledger (
		id TEXT PRIMARY KEY,
		tradespace TEXT NOT NULL,
		currency TEXT NOT NULL,
		amount TEXT NOT NULL,
		allocation_name TEXT NOT NULL,
		target_type TEXT NOT NULL,
		target_name TEXT NOT NULL,
		created_at TEXT NOT NULL,
		UNIQUE(tradespace, target_type, target_name)
	);

	CREATE INDEX IF NOT EXISTS idx_ledger_balance ON ledger(tradespace, currency);
	CREATE INDEX IF NOT EXISTS idx_ledger_tradespace ON ledger(tradespace);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_allocation ON ledger(tradespace, allocation_name);
	`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

// Append atomically checks balance and inserts a ledger entry.
// Uses BEGIN IMMEDIATE for proper write serialization in SQLite.
func (s *LedgerStorage) Append(ctx context.Context, e *storage.LedgerEntry) error {
	// Validate amount format
	if _, err := decimal.NewFromString(e.Amount); err != nil {
		return fmt.Errorf("invalid amount: %w", err)
	}

	// Always generate ID server-side
	b := make([]byte, 16)
	rand.Read(b)
	e.ID = fmt.Sprintf("%x", b)

	// Set created time
	now := time.Now().UTC()
	if e.CreatedAt.IsZero() {
		e.CreatedAt = now
	}

	// Use transaction for atomicity
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if exists, err := ledgerEntryExists(tx, ctx, `
		SELECT 1 FROM ledger
		WHERE tradespace = ? AND allocation_name = ?
	`, e.Tradespace, e.AllocationName); err != nil {
		return fmt.Errorf("failed to check existing allocation entry: %w", err)
	} else if exists {
		return storage.ErrAllocationApplied
	}

	if exists, err := ledgerEntryExists(tx, ctx, `
		SELECT 1 FROM ledger
		WHERE tradespace = ? AND target_type = ? AND target_name = ?
	`, e.Tradespace, e.TargetType, e.TargetName); err != nil {
		return fmt.Errorf("failed to check existing target entry: %w", err)
	} else if exists {
		return storage.ErrAlreadyAllocated
	}

	if amt, _ := decimal.NewFromString(e.Amount); amt.IsNegative() {
		balance, err := s.balanceTx(ctx, tx, e.Tradespace, e.Currency)
		if err != nil {
			return err
		}
		if balance.Add(amt).IsNegative() {
			return storage.ErrInsufficientBalance
		}
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO ledger (id, tradespace, currency, amount, allocation_name, target_type, target_name, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, e.ID, e.Tradespace, e.Currency, e.Amount, e.AllocationName, e.TargetType, e.TargetName, e.CreatedAt.Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("failed to insert ledger entry: %w", err)
	}

	return tx.Commit()
}

// Balance returns SUM(amount) for tradespace+currency.
func (s *LedgerStorage) Balance(ctx context.Context, tradespace, currency string) (string, error) {
	balance, err := s.balanceQuery(ctx, s.db, tradespace, currency)
	if err != nil {
		return "0", err
	}
	return balance.String(), nil
}

// GetByTarget returns entry for a target record.
func (s *LedgerStorage) GetByTarget(ctx context.Context, tradespace, targetType, targetName string) (*storage.LedgerEntry, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tradespace, currency, amount, allocation_name, target_type, target_name, created_at
		FROM ledger
		WHERE tradespace = ? AND target_type = ? AND target_name = ?
	`, tradespace, targetType, targetName)

	return scanLedgerEntry(row)
}

// GetByAllocation returns entry for an Allocation record.
func (s *LedgerStorage) GetByAllocation(ctx context.Context, tradespace, allocationName string) (*storage.LedgerEntry, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, tradespace, currency, amount, allocation_name, target_type, target_name, created_at
		FROM ledger
		WHERE tradespace = ? AND allocation_name = ?
	`, tradespace, allocationName)

	return scanLedgerEntry(row)
}

// List returns all entries for a tradespace.
func (s *LedgerStorage) List(ctx context.Context, tradespace string) ([]*storage.LedgerEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tradespace, currency, amount, allocation_name, target_type, target_name, created_at
		FROM ledger
		WHERE tradespace = ?
		ORDER BY created_at ASC
	`, tradespace)
	if err != nil {
		return nil, fmt.Errorf("failed to list ledger entries: %w", err)
	}
	defer rows.Close()

	var entries []*storage.LedgerEntry
	for rows.Next() {
		entry, err := scanLedgerEntryRows(rows)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, rows.Err()
}

// Close releases resources.
func (s *LedgerStorage) Close() error {
	return s.db.Close()
}

type ledgerQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *LedgerStorage) balanceTx(ctx context.Context, tx *sql.Tx, tradespace, currency string) (decimal.Decimal, error) {
	return s.balanceQuery(ctx, tx, tradespace, currency)
}

func (s *LedgerStorage) balanceQuery(ctx context.Context, q ledgerQueryer, tradespace, currency string) (decimal.Decimal, error) {
	var balanceStr sql.NullString
	err := q.QueryRowContext(ctx, `
		SELECT SUM(CAST(amount AS REAL)) FROM ledger
		WHERE tradespace = ? AND currency = ?
	`, tradespace, currency).Scan(&balanceStr)
	if err != nil {
		return decimal.Zero, fmt.Errorf("failed to get balance: %w", err)
	}

	if !balanceStr.Valid || balanceStr.String == "" {
		return decimal.Zero, nil
	}

	balance, err := decimal.NewFromString(balanceStr.String)
	if err != nil {
		var f float64
		if _, scanErr := fmt.Sscanf(balanceStr.String, "%e", &f); scanErr == nil {
			return decimal.NewFromFloat(f), nil
		}
		return decimal.Zero, nil
	}

	return balance, nil
}

func ledgerEntryExists(tx *sql.Tx, ctx context.Context, query string, args ...any) (bool, error) {
	var exists int
	err := tx.QueryRowContext(ctx, query, args...).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func scanLedgerEntry(row *sql.Row) (*storage.LedgerEntry, error) {
	var (
		id             string
		tradespace     string
		currency       string
		amount         string
		allocationName string
		targetType     string
		targetName     string
		createdAt      string
	)

	err := row.Scan(&id, &tradespace, &currency, &amount, &allocationName, &targetType, &targetName, &createdAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("failed to scan ledger entry: %w", err)
	}

	created, _ := time.Parse(time.RFC3339Nano, createdAt)

	return &storage.LedgerEntry{
		ID:             id,
		Tradespace:     tradespace,
		Currency:       currency,
		Amount:         amount,
		AllocationName: allocationName,
		TargetType:     targetType,
		TargetName:     targetName,
		CreatedAt:      created,
	}, nil
}

func scanLedgerEntryRows(rows *sql.Rows) (*storage.LedgerEntry, error) {
	var (
		id             string
		tradespace     string
		currency       string
		amount         string
		allocationName string
		targetType     string
		targetName     string
		createdAt      string
	)

	err := rows.Scan(&id, &tradespace, &currency, &amount, &allocationName, &targetType, &targetName, &createdAt)
	if err != nil {
		return nil, fmt.Errorf("failed to scan ledger entry: %w", err)
	}

	created, _ := time.Parse(time.RFC3339Nano, createdAt)

	return &storage.LedgerEntry{
		ID:             id,
		Tradespace:     tradespace,
		Currency:       currency,
		Amount:         amount,
		AllocationName: allocationName,
		TargetType:     targetType,
		TargetName:     targetName,
		CreatedAt:      created,
	}, nil
}
