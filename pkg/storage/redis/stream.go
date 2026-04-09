package redis

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/roysav/marketplane/pkg/storage"
)

var ErrNotFound = errors.New("key not found")

// StreamStorage implements storage.StreamStorage using Redis TimeSeries.
type StreamStorage struct {
	client *redis.Client
}

// NewStreamStorage creates a StreamStorage backed by Redis TimeSeries.
func NewStreamStorage(client *redis.Client) *StreamStorage {
	return &StreamStorage{client: client}
}

func (s *StreamStorage) Add(ctx context.Context, key string, value float64) error {
	return s.AddAt(ctx, key, time.Now(), value)
}

func (s *StreamStorage) AddAt(ctx context.Context, key string, ts time.Time, value float64) error {
	tsMillis := ts.UnixMilli()
	_, err := s.client.Do(ctx, "TS.ADD", tsKey(key), tsMillis, value).Result()
	if err != nil && isKeyNotFound(err) {
		// Create the timeseries if it doesn't exist
		_, err = s.client.Do(ctx, "TS.CREATE", tsKey(key)).Result()
		if err != nil && !isKeyExists(err) {
			return err
		}
		_, err = s.client.Do(ctx, "TS.ADD", tsKey(key), tsMillis, value).Result()
	}
	return err
}

func (s *StreamStorage) Latest(ctx context.Context, key string) (*storage.StreamEntry, error) {
	result, err := s.client.Do(ctx, "TS.GET", tsKey(key)).Result()
	if err != nil {
		if isKeyNotFound(err) || err == redis.Nil {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return parseTimeSeriesEntry(key, result)
}

func (s *StreamStorage) Range(ctx context.Context, key string, from, to time.Time) ([]*storage.StreamEntry, error) {
	fromMillis := from.UnixMilli()
	toMillis := to.UnixMilli()

	result, err := s.client.Do(ctx, "TS.RANGE", tsKey(key), fromMillis, toMillis).Result()
	if err != nil {
		if isKeyNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	items, ok := result.([]interface{})
	if !ok {
		return nil, nil
	}

	entries := make([]*storage.StreamEntry, 0, len(items))
	for _, item := range items {
		entry, err := parseTimeSeriesEntry(key, item)
		if err != nil {
			continue
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (s *StreamStorage) Watch(ctx context.Context, prefix string) (<-chan storage.WatchEvent, error) {
	// Redis TimeSeries doesn't have native pub/sub for new data points.
	// We use keyspace notifications on the timeseries keys.
	s.client.ConfigSet(ctx, "notify-keyspace-events", "KEA")

	pattern := "__keyspace@*__:" + tsKey(prefix) + "*"
	pubsub := s.client.PSubscribe(ctx, pattern)

	ch := make(chan storage.WatchEvent, 100)

	go func() {
		defer close(ch)
		defer pubsub.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-pubsub.Channel():
				if !ok {
					return
				}

				if msg.Payload != "ts.add" {
					continue
				}

				// Extract key and get latest value
				key := extractKeyFromChannel(msg.Channel, "ts:")
				entry, err := s.Latest(ctx, key)
				if err != nil {
					continue
				}

				event := storage.WatchEvent{
					Type:  storage.WatchEventAdd,
					Entry: entry,
				}

				select {
				case ch <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return ch, nil
}

func (s *StreamStorage) Close() error {
	return s.client.Close()
}

func tsKey(key string) string { return "ts:" + key }

func extractKeyFromChannel(channel, prefix string) string {
	// Channel format: __keyspace@0__:ts:mykey
	for i := 0; i < len(channel); i++ {
		if channel[i] == ':' {
			remaining := channel[i+1:]
			if len(remaining) > len(prefix) && remaining[:len(prefix)] == prefix {
				return remaining[len(prefix):]
			}
			return remaining
		}
	}
	return channel
}

func parseTimeSeriesEntry(key string, result interface{}) (*storage.StreamEntry, error) {
	// Result is [timestamp, value] array
	arr, ok := result.([]interface{})
	if !ok || len(arr) < 2 {
		return nil, errors.New("invalid timeseries entry format")
	}

	var tsMillis int64
	switch v := arr[0].(type) {
	case int64:
		tsMillis = v
	case string:
		tsMillis, _ = strconv.ParseInt(v, 10, 64)
	}

	var value float64
	switch v := arr[1].(type) {
	case float64:
		value = v
	case string:
		value, _ = strconv.ParseFloat(v, 64)
	}

	return &storage.StreamEntry{
		Key:       key,
		Value:     value,
		Timestamp: time.UnixMilli(tsMillis),
	}, nil
}

func isKeyNotFound(err error) bool {
	return err != nil && (err.Error() == "ERR TSDB: the key does not exist" ||
		errors.Is(err, redis.Nil))
}

func isKeyExists(err error) bool {
	return err != nil && err.Error() == "ERR TSDB: key already exists"
}
