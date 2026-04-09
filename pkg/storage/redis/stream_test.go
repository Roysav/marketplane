package redis

import (
	"context"
	"testing"
	"time"
)

func TestStreamStorage(t *testing.T) {
	ctx := context.Background()

	client, err := NewClient(ctx, Options{Addr: "localhost:6379"})
	if err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}
	defer client.Close()

	// Clean up test keys
	client.Do(ctx, "DEL", "ts:test/ticker")

	s := NewStreamStorage(client)

	t.Run("Add and Latest", func(t *testing.T) {
		err := s.Add(ctx, "test/ticker", 100.5)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}

		entry, err := s.Latest(ctx, "test/ticker")
		if err != nil {
			t.Fatalf("Latest failed: %v", err)
		}

		if entry.Key != "test/ticker" {
			t.Errorf("expected key test/ticker, got %s", entry.Key)
		}
		if entry.Value != 100.5 {
			t.Errorf("expected value 100.5, got %f", entry.Value)
		}
	})

	t.Run("AddAt with specific timestamp", func(t *testing.T) {
		client.Do(ctx, "DEL", "ts:test/ticker2")

		ts := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		err := s.AddAt(ctx, "test/ticker2", ts, 200.0)
		if err != nil {
			t.Fatalf("AddAt failed: %v", err)
		}

		entry, err := s.Latest(ctx, "test/ticker2")
		if err != nil {
			t.Fatalf("Latest failed: %v", err)
		}

		if entry.Value != 200.0 {
			t.Errorf("expected value 200.0, got %f", entry.Value)
		}
		if !entry.Timestamp.Equal(ts) {
			t.Errorf("expected timestamp %v, got %v", ts, entry.Timestamp)
		}
	})

	t.Run("Range", func(t *testing.T) {
		client.Do(ctx, "DEL", "ts:test/range")

		base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		s.AddAt(ctx, "test/range", base, 1.0)
		s.AddAt(ctx, "test/range", base.Add(time.Second), 2.0)
		s.AddAt(ctx, "test/range", base.Add(2*time.Second), 3.0)
		s.AddAt(ctx, "test/range", base.Add(3*time.Second), 4.0)

		entries, err := s.Range(ctx, "test/range", base, base.Add(2*time.Second))
		if err != nil {
			t.Fatalf("Range failed: %v", err)
		}

		if len(entries) != 3 {
			t.Fatalf("expected 3 entries, got %d", len(entries))
		}

		if entries[0].Value != 1.0 || entries[1].Value != 2.0 || entries[2].Value != 3.0 {
			t.Errorf("unexpected values: %v", entries)
		}
	})

	t.Run("Latest not found", func(t *testing.T) {
		_, err := s.Latest(ctx, "test/nonexistent")
		if err != ErrNotFound {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestStreamStorageWatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := NewClient(ctx, Options{Addr: "localhost:6379"})
	if err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}
	defer client.Close()

	client.Do(ctx, "DEL", "ts:test/watch")

	s := NewStreamStorage(client)

	// Start watching
	watchCh, err := s.Watch(ctx, "test/watch")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Give pubsub time to subscribe
	time.Sleep(100 * time.Millisecond)

	// Add a value
	err = s.Add(ctx, "test/watch", 42.0)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for event
	select {
	case event := <-watchCh:
		if event.Entry.Value != 42.0 {
			t.Errorf("expected value 42.0, got %f", event.Entry.Value)
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for watch event")
	}
}
