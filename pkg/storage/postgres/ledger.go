package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/storage"
)

// LedgerStorage implements storage.LedgerStorage using PostgreSQL.
type LedgerStorage struct {
	pool *pgxpool.Pool
}

// NewLedgerStorage creates a LedgerStorage from an existing connection Pool.
func NewLedgerStorage(pool *pgxpool.Pool) *LedgerStorage {
	return &LedgerStorage{pool: pool}
}

func (s *LedgerStorage) Append(ctx context.Context, prefix, key, amount, currency string) error {
	lockKey := fmt.Sprintf("ledger:%s:%s", prefix, currency)
	likePrefix := prefix + "%"

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ledger: begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Acquire a transaction-scoped advisory lock on tradespace+currency.
	// This serializes all appends for the same tradespace+currency pair.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, lockKey); err != nil {
		return fmt.Errorf("ledger: advisory lock: %w", err)
	}

	// Compute the current balance for this tradespace+currency.
	var currentBalance string
	err = tx.QueryRow(ctx,
		`SELECT COALESCE(SUM(amount), 0)::TEXT FROM ledger WHERE key LIKE $1 AND currency = $2`,
		likePrefix, currency,
	).Scan(&currentBalance)
	if err != nil {
		return fmt.Errorf("ledger: sum balance: %w", err)
	}

	// Check if balance + new amount >= 0 and compute new remaining, all in SQL
	// to avoid floating-point issues in Go.
	var allowed bool
	var remaining string
	err = tx.QueryRow(ctx,
		`SELECT ($1::DECIMAL + $2::DECIMAL) >= 0, ($1::DECIMAL + $2::DECIMAL)::TEXT`,
		currentBalance, amount,
	).Scan(&allowed, &remaining)
	if err != nil {
		return fmt.Errorf("ledger: check balance: %w", err)
	}

	if !allowed {
		return fmt.Errorf("%w: %s balance would be %s", storage.ErrInsufficientBalance, currency, remaining)
	}

	// Insert the ledger entry with the computed remaining balance.
	if _, err := tx.Exec(ctx,
		`INSERT INTO ledger (key, amount, remaining, currency) VALUES ($1, $2::DECIMAL, $3::DECIMAL, $4)`,
		key, amount, remaining, currency,
	); err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: %s", storage.ErrAlreadyExists, key)
		}
		return fmt.Errorf("ledger: insert: %w", err)
	}

	return tx.Commit(ctx)
}

// List returns all ledger entries for a tradespace, ordered by key.
func (s *LedgerStorage) List(ctx context.Context, tradespace string) ([]*storage.LedgerEntry, error) {
	prefix := record.Key("core/v1/Allocation", tradespace, "") + "%"

	rows, err := s.pool.Query(ctx,
		`SELECT key, amount::TEXT, remaining::TEXT, currency FROM ledger WHERE key LIKE $1 ORDER BY key`,
		prefix,
	)
	if err != nil {
		return nil, fmt.Errorf("ledger: list: %w", err)
	}
	defer rows.Close()

	var entries []*storage.LedgerEntry
	for rows.Next() {
		e := &storage.LedgerEntry{}
		if err := rows.Scan(&e.Key, &e.Amount, &e.Remaining, &e.Currency); err != nil {
			return nil, fmt.Errorf("ledger: list scan: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// Balance returns the current balance for a tradespace+currency.
func (s *LedgerStorage) Balance(ctx context.Context, tradespace, currency string) (string, error) {
	prefix := record.Key("core/v1/Allocation", tradespace, "") + "%"

	var balance string
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(amount), 0)::TEXT FROM ledger WHERE key LIKE $1 AND currency = $2`,
		prefix, currency,
	).Scan(&balance)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "0", nil
		}
		return "", fmt.Errorf("ledger: balance: %w", err)
	}
	return balance, nil
}
