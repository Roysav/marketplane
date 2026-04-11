package redis

import (
	"context"
	"testing"
	"time"
)

func TestEventStorage(t *testing.T) {
	ctx := context.Background()

	client, err := NewClient(ctx, Options{Addr: "localhost:6379", DB: 12})
	if err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}
	defer client.Close()
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("failed to clear redis state: %v", err)
	}

	// Clean up test stream
	client.Del(ctx, "events:test/topic")

	e := NewEventStorage(client)

	t.Run("Publish", func(t *testing.T) {
		id, err := e.Publish(ctx, "test/topic", `{"action":"created","id":1}`)
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
		if id == "" {
			t.Error("expected non-empty event ID")
		}
	})

	t.Run("SubscribeFrom beginning", func(t *testing.T) {
		// Publish a few more events
		e.Publish(ctx, "test/topic", `{"action":"updated","id":1}`)
		e.Publish(ctx, "test/topic", `{"action":"deleted","id":1}`)

		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		ch, err := e.SubscribeFrom(ctx, "test/topic", "0")
		if err != nil {
			t.Fatalf("SubscribeFrom failed: %v", err)
		}

		var events []string
		for i := 0; i < 3; i++ {
			select {
			case event := <-ch:
				events = append(events, event.Data)
			case <-ctx.Done():
				t.Fatalf("timeout, only received %d events", len(events))
			}
		}

		if len(events) != 3 {
			t.Errorf("expected 3 events, got %d", len(events))
		}
	})
}

func TestEventStorageSubscribe(t *testing.T) {
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

	// Use a unique topic
	topic := "test/live-" + time.Now().Format("150405")

	e := NewEventStorage(client)

	// Start subscribing (from latest)
	ch, err := e.Subscribe(ctx, topic)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Give subscriber time to start
	time.Sleep(100 * time.Millisecond)

	// Publish after subscribing
	_, err = e.Publish(ctx, topic, `{"live":"event"}`)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Should receive the event
	select {
	case event := <-ch:
		if event.Data != `{"live":"event"}` {
			t.Errorf("unexpected data: %s", event.Data)
		}
		if event.Topic != topic {
			t.Errorf("expected topic %s, got %s", topic, event.Topic)
		}
	case <-time.After(3 * time.Second):
		t.Error("timeout waiting for event")
	}
}
