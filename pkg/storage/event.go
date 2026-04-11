// Package storage defines the event storage interface for publish/subscribe
// messaging. EventStorage is backed by Redis Streams and provides durable,
// ordered delivery of record change events to subscribers.
package storage

import "context"

// Event is a message in an event stream.
type Event struct {
	ID    string // unique event ID (e.g., Redis stream ID "1234567890123-0")
	Topic string // the topic/stream name
	Data  string // JSON blob
}

// EventStorage provides publish/subscribe messaging.
type EventStorage interface {
	// Publish sends an event to a topic.
	Publish(ctx context.Context, topic string, data string) (eventID string, err error)

	// Subscribe returns a channel of events for a topic.
	// Starts from the latest events by default.
	// The channel is closed when the context is canceled.
	Subscribe(ctx context.Context, topic string) (<-chan Event, error)

	// SubscribeFrom subscribes starting from a specific event ID.
	// Use "0" to read from the beginning.
	SubscribeFrom(ctx context.Context, topic string, fromID string) (<-chan Event, error)

	// Close releases resources.
	Close() error
}
