package storage

import (
	"context"
	"errors"
)

var ErrInsufficientBalance = errors.New("insufficient balance")

// LedgerEntry represents a single allocation entry in the ledger.
type LedgerEntry struct {
	Key       string // full record key, e.g. core/v1/Allocation/prod/order-123
	Amount    string // decimal string, negative for spending
	Remaining string // balance after this entry
	Currency  string
}

// LedgerStorage provides append-only ledger operations.
type LedgerStorage interface {
	// Append atomically adds a ledger entry. It locks all entries for the
	// tradespace+currency, computes the current balance, and only inserts
	// if the resulting balance would be >= 0. Returns ErrInsufficientBalance
	// otherwise.
	Append(ctx context.Context, prefix, key, amount, currency string) error

	// List returns all ledger entries for a tradespace.
	List(ctx context.Context, tradespace string) ([]*LedgerEntry, error)
}
