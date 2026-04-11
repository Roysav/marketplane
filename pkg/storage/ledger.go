// Package storage defines the ledger storage interface.
// LedgerStorage manages an append-only log of approved fund allocations,
// providing atomic balance checks to prevent overdrafts across tradespaces.
package storage

import (
	"context"
	"errors"
	"time"
)

var (
	ErrInsufficientBalance = errors.New("insufficient balance")
	ErrAlreadyAllocated    = errors.New("target already has an allocation")
)

// LedgerEntry is an approved allocation in the ledger table.
type LedgerEntry struct {
	ID             string    // unique entry ID
	Tradespace     string    // tradespace this entry belongs to
	Currency       string    // currency code (e.g., "USD")
	Amount         string    // decimal string, can be positive or negative
	AllocationName string    // references the Allocation record
	TargetType     string    // type of the target record (e.g., "polymarket/v1/Order")
	TargetName     string    // name of the target record
	CreatedAt      time.Time // when the entry was created
}

// LedgerStorage manages the append-only ledger of approved allocations.
type LedgerStorage interface {
	// Append atomically checks balance and inserts a ledger entry.
	// Returns ErrInsufficientBalance if SUM + amount < 0.
	// Returns ErrAlreadyAllocated if target already has an entry.
	Append(ctx context.Context, entry *LedgerEntry) error

	// Balance returns SUM(amount) for tradespace+currency.
	Balance(ctx context.Context, tradespace, currency string) (string, error)

	// GetByTarget returns entry for a target record.
	GetByTarget(ctx context.Context, tradespace, targetType, targetName string) (*LedgerEntry, error)

	// List returns all entries for a tradespace.
	List(ctx context.Context, tradespace string) ([]*LedgerEntry, error)

	// Close releases resources.
	Close() error
}
