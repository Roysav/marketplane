// Package storage defines the storage interfaces.
package storage

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrConflict      = errors.New("conflict")
)

// Key uniquely identifies a row.
type Key struct {
	Type       string // e.g. "core/v1/Tradespace"
	Tradespace string
	Name       string
}

// Row is a stored record in the database.
type Row struct {
	Type       string // e.g. "core/v1/Tradespace"
	Tradespace string
	Name       string
	Labels     map[string]string
	Data       string // JSON blob

	ResourceVersion int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// Key returns the row's key.
func (r *Row) Key() Key {
	return Key{
		Type:       r.Type,
		Tradespace: r.Tradespace,
		Name:       r.Name,
	}
}

// Query specifies filtering for List.
type Query struct {
	Type       string            // required, e.g. "core/v1/Tradespace"
	Tradespace string            // optional
	Labels     map[string]string // optional
	Limit      int
}

// RowStorage persists rows.
type RowStorage interface {
	Create(ctx context.Context, r *Row) (*Row, error)
	Get(ctx context.Context, key Key) (*Row, error)
	Update(ctx context.Context, r *Row) (*Row, error)
	Delete(ctx context.Context, key Key) error
	List(ctx context.Context, q Query) ([]*Row, error)
	Close() error
}
