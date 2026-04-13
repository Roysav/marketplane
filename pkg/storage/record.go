// Package storage, Record storage
package storage

import (
	"context"
	"errors"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
)

type Row struct {
	Key    string
	Labels map[string]string
	Data   []byte // JSON blob
}

// Query specifies filtering for List.
type Query struct {
	Prefix string
	Labels map[string]string
	Limit  int
}

// RowStorage persists rows.
type RowStorage interface {
	Create(ctx context.Context, r *Row) (*Row, error)
	Get(ctx context.Context, key string) (*Row, error)
	Update(ctx context.Context, r *Row) (*Row, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, q Query) ([]*Row, error)
	Close() error
}
