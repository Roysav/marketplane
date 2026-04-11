package redis

import (
	"context"
	"testing"
	"time"
)

func TestStreamStorage(t *testing.T) {
	ctx := context.Background()

	client, err := NewClient(ctx, Options{Addr: "localhost:6379", DB: 12})
	if err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}
	defer client.Close()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("failed to clear redis state: %v", err)
	}

	// Clean up test keys
	client.Del(ctx, "stream:test/ticker")

	s := NewStreamStorage(client)

	t.Run("Add and Latest", func(t *testing.T) {
		ts := time.Now()
		err := s.Add(ctx, "test/ticker", ts, `{"bid": 100.5, "ask": 100.6}`)
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
		if entry.Value != `{"bid": 100.5, "ask": 100.6}` {
			t.Errorf("expected JSON value, got %s", entry.Value)
		}
	})

	t.Run("Add with specific timestamp", func(t *testing.T) {
		client.Del(ctx, "stream:test/ticker2")

		ts := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		err := s.Add(ctx, "test/ticker2", ts, `{"price": 200.0}`)
		if err != nil {
			t.Fatalf("Add failed: %v", err)
		}

		entry, err := s.Latest(ctx, "test/ticker2")
		if err != nil {
			t.Fatalf("Latest failed: %v", err)
		}

		if entry.Value != `{"price": 200.0}` {
			t.Errorf("expected JSON value, got %s", entry.Value)
		}
		// Timestamp should be within a second of what we set (millisecond precision in stream ID)
		if entry.Timestamp.Sub(ts).Abs() > time.Second {
			t.Errorf("expected timestamp %v, got %v", ts, entry.Timestamp)
		}
	})

	t.Run("Range", func(t *testing.T) {
		client.Del(ctx, "stream:test/range")

		base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		s.Add(ctx, "test/range", base, `{"v": 1}`)
		s.Add(ctx, "test/range", base.Add(time.Second), `{"v": 2}`)
		s.Add(ctx, "test/range", base.Add(2*time.Second), `{"v": 3}`)
		s.Add(ctx, "test/range", base.Add(3*time.Second), `{"v": 4}`)

		entries, err := s.Range(ctx, "test/range", base, base.Add(2*time.Second))
		if err != nil {
			t.Fatalf("Range failed: %v", err)
		}

		if len(entries) != 3 {
			t.Fatalf("expected 3 entries, got %d", len(entries))
		}

		if entries[0].Value != `{"v": 1}` || entries[1].Value != `{"v": 2}` || entries[2].Value != `{"v": 3}` {
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

	client, err := NewClient(ctx, Options{Addr: "localhost:6379", DB: 12})
	if err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}
	defer client.Close()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("failed to clear redis state: %v", err)
	}

	client.Del(ctx, "stream:test/watch")

	s := NewStreamStorage(client)

	// Start watching
	watchCh, err := s.Watch(ctx, "test/watch")
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Give watcher time to start
	time.Sleep(100 * time.Millisecond)

	// Add a value
	ts := time.Now()
	err = s.Add(ctx, "test/watch", ts, `{"event": "test"}`)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// Wait for event
	select {
	case event := <-watchCh:
		if event.Entry.Value != `{"event": "test"}` {
			t.Errorf("expected JSON value, got %s", event.Entry.Value)
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for watch event")
	}
}
