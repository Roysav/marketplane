package service

import (
	"context"
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/storage"
	"github.com/roysav/marketplane/pkg/storage/redis"
	"github.com/roysav/marketplane/pkg/storage/sqlite"
)

func newTestStreamService(t *testing.T) (*StreamService, *goredis.Client) {
	t.Helper()
	ctx := context.Background()

	rows, err := sqlite.New(ctx, ":memory:")
	if err != nil {
		t.Fatalf("failed to create sqlite storage: %v", err)
	}
	t.Cleanup(func() { rows.Close() })

	redisClient, err := redis.NewClient(ctx, redis.Options{Addr: "localhost:6379", DB: 11})
	if err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}
	t.Cleanup(func() { redisClient.Close() })
	if err := redisClient.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("failed to clear redis state: %v", err)
	}

	streams := redis.NewStreamStorage(redisClient)

	return NewStreamService(StreamServiceConfig{
		Rows:    rows,
		Streams: streams,
	}), redisClient
}

func newTestStreamServices(t *testing.T) (*Service, *StreamService, *goredis.Client) {
	t.Helper()
	ctx := context.Background()

	rows, err := sqlite.New(ctx, ":memory:")
	if err != nil {
		t.Fatalf("failed to create sqlite storage: %v", err)
	}
	t.Cleanup(func() { rows.Close() })

	redisClient, err := redis.NewClient(ctx, redis.Options{Addr: "localhost:6379", DB: 11})
	if err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}
	t.Cleanup(func() { redisClient.Close() })
	if err := redisClient.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("failed to clear redis state: %v", err)
	}

	streams := redis.NewStreamStorage(redisClient)

	recordSvc := New(Config{Rows: rows})
	streamSvc := NewStreamService(StreamServiceConfig{
		Rows:    rows,
		Streams: streams,
	})

	return recordSvc, streamSvc, redisClient
}

func createTestStreamDefinition(t *testing.T, s *StreamService, name string) StreamKey {
	t.Helper()
	ctx := context.Background()

	_, err := s.rows.Create(ctx, &storage.Row{
		Type:       "core/v1/StreamDefinition",
		Tradespace: "default",
		Name:       name,
		Data: `{
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
		}`,
	})
	if err != nil {
		t.Fatalf("failed to create stream definition: %v", err)
	}

	return StreamKey{
		Name: name,
	}
}

func TestStreamService_AppendAndLatest(t *testing.T) {
	ctx := context.Background()
	s, redisClient := newTestStreamService(t)

	key := createTestStreamDefinition(t, s, "btcusdt")

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
	s, _ := newTestStreamService(t)

	key := createTestStreamDefinition(t, s, "ethusdt")

	// Append without required field should fail
	ts := time.Now()
	err := s.Append(ctx, key, ts, map[string]any{
		"volume": "1000",
	})
	if err == nil {
		t.Error("expected validation error, got nil")
	}
	if !errors.Is(err, ErrStreamValidation) {
		t.Errorf("expected ErrStreamValidation, got: %v", err)
	}
}

func TestStreamService_Range(t *testing.T) {
	ctx := context.Background()
	s, redisClient := newTestStreamService(t)

	key := createTestStreamDefinition(t, s, "solusdt")

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

	s, redisClient := newTestStreamService(t)

	key := createTestStreamDefinition(t, s, "avaxusdt")

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
	s, _ := newTestStreamService(t)

	key := StreamKey{
		Name: "missing",
	}

	_, err := s.Latest(ctx, key)
	if err == nil {
		t.Error("expected error, got nil")
	}
	if !errors.Is(err, ErrStreamNotFound) {
		t.Errorf("expected ErrStreamNotFound, got: %v", err)
	}
}

func TestStreamService_EmptyStream(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStreamService(t)

	createTestStreamDefinition(t, s, "linkusdt")

	key := StreamKey{
		Name: "linkusdt",
	}

	_, err := s.Latest(ctx, key)
	if err == nil {
		t.Error("expected error for empty stream, got nil")
	}
	if !errors.Is(err, ErrStreamEmpty) {
		t.Errorf("expected ErrStreamEmpty, got: %v", err)
	}
}

func TestStreamService_RecordCreatedStreamDefinition(t *testing.T) {
	ctx := context.Background()
	recordSvc, streamSvc, redisClient := newTestStreamServices(t)

	key := StreamKey{Name: "polymarket.crypto.binance.btcusdt"}
	redisClient.Del(ctx, "stream:"+key.String())

	_, err := recordSvc.Create(ctx, &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "StreamDefinition"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       key.Name,
		},
		Spec: map[string]any{
			"group":     "polymarket",
			"version":   "v1",
			"kind":      "CryptoPrice",
			"retention": "24h",
			"schema": map[string]any{
				"type":     "object",
				"required": []any{"value"},
				"properties": map[string]any{
					"value": map[string]any{"type": "number"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to create StreamDefinition through service: %v", err)
	}

	err = streamSvc.Append(ctx, key, time.Now(), map[string]any{"value": 42.5})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	entry, err := streamSvc.Latest(ctx, key)
	if err != nil {
		t.Fatalf("Latest failed: %v", err)
	}

	if entry.Key != key.Name {
		t.Fatalf("unexpected stream key: %s", entry.Key)
	}
}

func TestStreamService_DisabledWithoutStorage(t *testing.T) {
	ctx := context.Background()

	rows, err := sqlite.New(ctx, ":memory:")
	if err != nil {
		t.Fatalf("failed to create sqlite storage: %v", err)
	}
	defer rows.Close()

	streamSvc := NewStreamService(StreamServiceConfig{
		Rows:    rows,
		Streams: nil,
	})

	err = streamSvc.Append(ctx, StreamKey{Name: "disabled"}, time.Now(), map[string]any{"value": 1})
	if !errors.Is(err, ErrStreamsDisabled) {
		t.Fatalf("expected ErrStreamsDisabled, got %v", err)
	}
}
