package storage

import (
	"context"
	"time"
)

// StreamEntry is a data point in a timeseries.
type StreamEntry struct {
	Key       string
	Value     float64
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
	// Add appends a data point to the series (auto-timestamped).
	Add(ctx context.Context, key string, value float64) error

	// AddAt appends a data point with a specific timestamp.
	AddAt(ctx context.Context, key string, ts time.Time, value float64) error

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
