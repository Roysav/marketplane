// Package storage defines the storage interfaces for timeseries data.
// StreamStorage provides append, query, and watch operations backed by
// Redis TimeSeries. It is used for real-time market data such as price
// feeds, order book snapshots, and other high-frequency series.
package storage

import (
	"context"
	"time"
)

// StreamEntry is a data point in a timeseries.
type StreamEntry struct {
	Key       string
	Value     string // JSON blob
	Timestamp time.Time
}

// WatchEventType indicates what happened.
type WatchEventType int

const (
	WatchEventAdd WatchEventType = iota
)

// WatchEvent represents a new data point.
type WatchEvent struct {
	Type  WatchEventType
	Entry *StreamEntry
}

// StreamStorage provides timeseries storage with watch capability.
type StreamStorage interface {
	// Add appends a data point to the series with the given timestamp.
	Add(ctx context.Context, key string, ts time.Time, value string) error

	// Latest gets the most recent value for a key.
	Latest(ctx context.Context, key string) (*StreamEntry, error)

	// Range gets values between timestamps.
	Range(ctx context.Context, key string, from, to time.Time) ([]*StreamEntry, error)

	// Watch notifies on new data points for keys matching prefix.
	// The channel is closed when the context is canceled.
	Watch(ctx context.Context, prefix string) (<-chan WatchEvent, error)

	// Close releases resources.
	Close() error
}
