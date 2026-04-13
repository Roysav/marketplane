package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/service"
	"github.com/roysav/marketplane/pkg/storage"
	"github.com/roysav/marketplane/pkg/storage/postgres"
	"github.com/roysav/marketplane/pkg/storage/redis"
	"github.com/roysav/marketplane/tests"
)

func newTestStreamService(t *testing.T) (*service.StreamService, storage.RowStorage, *goredis.Client) {
	t.Helper()
	ctx := context.Background()

	pool := tests.Pool(ctx, t)
	rows := postgres.New(pool)

	redisClient, err := redis.NewClient(ctx, redis.Options{Addr: "localhost:6379"})
	if err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}
	t.Cleanup(func() { redisClient.Close() })

	streams := redis.NewStreamStorage(redisClient)

	svc := service.NewStreamService(service.StreamServiceConfig{
		Rows:    rows,
		Streams: streams,
	})
	return svc, rows, redisClient
}

func createTestStreamDefinition(t *testing.T, rows storage.RowStorage, name string) service.StreamKey {
	t.Helper()
	ctx := context.Background()

	_, err := rows.Create(ctx, &storage.Row{
		Key: record.Key("core/v1/StreamDefinition", "default", name),
		Data: []byte(`{
			"group": "Binance.MarketFeed",
			"version": "v1alpha1",
			"kind": "Quotes",
			"schema": {
				"type": "object",
				"required": ["last_price"],
				"properties": {
					"last_price": {"type": "string"}
				}
			}
		}`),
	})
	if err != nil {
		t.Fatalf("failed to create stream definition: %v", err)
	}

	return service.StreamKey{
		Tradespace: "default",
		Name:       name,
	}
}

func TestStreamService_AppendAndLatest(t *testing.T) {
	ctx := context.Background()
	s, rows, redisClient := newTestStreamService(t)

	key := createTestStreamDefinition(t, rows, "btcusdt")

	// Clean up test stream
	redisClient.Del(ctx, "stream:"+key.String())

	// Append data
	ts := time.Now()
	err := s.Append(ctx, key, ts, map[string]any{
		"last_price": "67432.50",
	})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Get latest
	entry, err := s.Latest(ctx, key)
	if err != nil {
		t.Fatalf("Latest failed: %v", err)
	}

	if entry.Value != `{"last_price":"67432.50"}` {
		t.Errorf("unexpected value: %s", entry.Value)
	}
}

func TestStreamService_AppendValidation(t *testing.T) {
	ctx := context.Background()
	s, rows, _ := newTestStreamService(t)

	key := createTestStreamDefinition(t, rows, "ethusdt")

	// Append without required field should fail
	ts := time.Now()
	err := s.Append(ctx, key, ts, map[string]any{
		"volume": "1000",
	})
	if err == nil {
		t.Error("expected validation error, got nil")
	}
	if !errors.Is(err, service.ErrStreamValidation) {
		t.Errorf("expected ErrStreamValidation, got: %v", err)
	}
}

func TestStreamService_Range(t *testing.T) {
	ctx := context.Background()
	s, rows, redisClient := newTestStreamService(t)

	key := createTestStreamDefinition(t, rows, "solusdt")

	// Clean up test stream
	redisClient.Del(ctx, "stream:"+key.String())

	base := time.Now().Truncate(time.Second)

	// Add multiple entries
	for i := 0; i < 5; i++ {
		ts := base.Add(time.Duration(i) * time.Second)
		err := s.Append(ctx, key, ts, map[string]any{
			"last_price": "150.00",
		})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Query range (first 3 entries)
	entries, err := s.Range(ctx, key, base, base.Add(2*time.Second))
	if err != nil {
		t.Fatalf("Range failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(entries))
	}
}

func TestStreamService_Watch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, rows, redisClient := newTestStreamService(t)

	key := createTestStreamDefinition(t, rows, "avaxusdt")

	// Clean up test stream
	redisClient.Del(ctx, "stream:"+key.String())

	// Start watching
	ch, err := s.Watch(ctx, key)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// Give watcher time to start
	time.Sleep(100 * time.Millisecond)

	// Append data
	ts := time.Now()
	err = s.Append(ctx, key, ts, map[string]any{
		"last_price": "35.50",
	})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Wait for event
	select {
	case event := <-ch:
		if event.Entry.Value != `{"last_price":"35.50"}` {
			t.Errorf("unexpected value: %s", event.Entry.Value)
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for watch event")
	}
}

func TestStreamService_NotFound(t *testing.T) {
	ctx := context.Background()
	s, _, _ := newTestStreamService(t)

	key := service.StreamKey{
		Tradespace: "default",
		Name:       "missing",
	}

	_, err := s.Latest(ctx, key)
	if err == nil {
		t.Error("expected error, got nil")
	}
	if !errors.Is(err, service.ErrStreamNotFound) {
		t.Errorf("expected ErrStreamNotFound, got: %v", err)
	}
}

func TestStreamService_KeyMismatch(t *testing.T) {
	ctx := context.Background()
	s, rows, _ := newTestStreamService(t)

	// Create a stream definition
	createTestStreamDefinition(t, rows, "linkusdt")

	// Try to access — definition exists but no data appended to Redis stream
	key := service.StreamKey{
		Tradespace: "default",
		Name:       "linkusdt",
	}

	_, err := s.Latest(ctx, key)
	if err == nil {
		t.Error("expected error for mismatched key, got nil")
	}
	if !errors.Is(err, service.ErrStreamNotFound) {
		t.Errorf("expected ErrStreamNotFound, got: %v", err)
	}
}