// Package storage defines the record storage interface.
package storage

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
)

// Key uniquely identifies a record.
type Key struct {
	Type       string // e.g. "core/v1/Tradespace"
	Tradespace string
	Name       string
}

// Record is a stored entity.
type Record struct {
	Type       string // e.g. "core/v1/Tradespace"
	Tradespace string
	Name       string
	Labels     map[string]string
	Data       string // JSON blob

	ResourceVersion int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// Key returns the record's key.
func (r *Record) Key() Key {
	return Key{
		Type:       r.Type,
		Tradespace: r.Tradespace,
		Name:       r.Name,
	}
}

// Query specifies filtering for List.
type Query struct {
	Type       string // required, e.g. "core/v1/Tradespace"
	Tradespace string // optional
	Labels     map[string]string // optional
	Limit      int
}

// RecordStorage persists records.
type RecordStorage interface {
	Create(ctx context.Context, r *Record) (*Record, error)
	Get(ctx context.Context, key Key) (*Record, error)
	Update(ctx context.Context, r *Record) (*Record, error)
	Delete(ctx context.Context, key Key) error
	List(ctx context.Context, q Query) ([]*Record, error)
	Close() error
}
