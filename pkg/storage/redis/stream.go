package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/roysav/marketplane/pkg/storage"
)

var ErrNotFound = errors.New("key not found")

// StreamStorage implements storage.StreamStorage using Redis Streams.
type StreamStorage struct {
	client *redis.Client
}

// NewStreamStorage creates a StreamStorage backed by Redis Streams.
func NewStreamStorage(client *redis.Client) *StreamStorage {
	return &StreamStorage{client: client}
}

func (s *StreamStorage) Add(ctx context.Context, key string, ts time.Time, value string) error {
	// Use user-provided timestamp as the stream ID
	streamID := fmt.Sprintf("%d-*", ts.UnixMilli())

	_, err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: timeseriesKey(key),
		ID:     streamID,
		Values: map[string]interface{}{
			"data": value,
			"ts":   ts.UnixNano(),
		},
	}).Result()

	return err
}

func (s *StreamStorage) Latest(ctx context.Context, key string) (*storage.StreamEntry, error) {
	result, err := s.client.XRevRangeN(ctx, timeseriesKey(key), "+", "-", 1).Result()
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, ErrNotFound
	}

	return parseStreamMessage(key, result[0])
}

func (s *StreamStorage) Range(ctx context.Context, key string, from, to time.Time) ([]*storage.StreamEntry, error) {
	fromID := fmt.Sprintf("%d", from.UnixMilli())
	toID := fmt.Sprintf("%d", to.UnixMilli())

	result, err := s.client.XRange(ctx, timeseriesKey(key), fromID, toID).Result()
	if err != nil {
		return nil, err
	}

	entries := make([]*storage.StreamEntry, 0, len(result))
	for _, msg := range result {
		entry, err := parseStreamMessage(key, msg)
		if err != nil {
			continue
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (s *StreamStorage) Watch(ctx context.Context, prefix string) (<-chan storage.WatchEvent, error) {
	ch := make(chan storage.WatchEvent, 100)

	go func() {
		defer close(ch)

		lastID := "$"
		streamPattern := timeseriesKey(prefix)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			streams, err := s.client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{streamPattern, lastID},
				Count:   100,
				Block:   time.Second,
			}).Result()

			if err != nil {
				if err == redis.Nil {
					continue
				}
				if ctx.Err() != nil {
					return
				}
				continue
			}

			for _, stream := range streams {
				key := strings.TrimPrefix(stream.Stream, "stream:")

				for _, msg := range stream.Messages {
					entry, err := parseStreamMessage(key, msg)
					if err != nil {
						continue
					}

					event := storage.WatchEvent{
						Type:  storage.WatchEventAdd,
						Entry: entry,
					}

					select {
					case ch <- event:
						lastID = msg.ID
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return ch, nil
}

func (s *StreamStorage) Close() error {
	return s.client.Close()
}

func timeseriesKey(key string) string { return "stream:" + key }

func parseStreamMessage(key string, msg redis.XMessage) (*storage.StreamEntry, error) {
	data, ok := msg.Values["data"].(string)
	if !ok {
		return nil, errors.New("missing data field")
	}

	var ts time.Time
	if tsNano, ok := msg.Values["ts"].(string); ok {
		var nanos int64
		fmt.Sscanf(tsNano, "%d", &nanos)
		ts = time.Unix(0, nanos)
	} else {
		var millis int64
		fmt.Sscanf(msg.ID, "%d", &millis)
		ts = time.UnixMilli(millis)
	}

	return &storage.StreamEntry{
		Key:       key,
		Value:     data,
		Timestamp: ts,
	}, nil
}
