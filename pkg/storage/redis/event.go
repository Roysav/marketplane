package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/roysav/marketplane/pkg/storage"
)

// EventStorage implements storage.EventStorage using Redis Streams.
type EventStorage struct {
	client *redis.Client
}

// NewEventStorage creates an EventStorage backed by Redis Streams.
func NewEventStorage(client *redis.Client) *EventStorage {
	return &EventStorage{client: client}
}

func (e *EventStorage) Publish(ctx context.Context, topic string, data string) (string, error) {
	id, err := e.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey(topic),
		Values: map[string]interface{}{"data": data},
	}).Result()
	if err != nil {
		return "", err
	}
	return id, nil
}

func (e *EventStorage) Subscribe(ctx context.Context, topic string) (<-chan storage.Event, error) {
	return e.SubscribeFrom(ctx, topic, "$") // "$" means latest
}

func (e *EventStorage) SubscribeFrom(ctx context.Context, topic string, fromID string) (<-chan storage.Event, error) {
	ch := make(chan storage.Event, 100)

	go func() {
		defer close(ch)

		lastID := fromID
		key := streamKey(topic)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			streams, err := e.client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{key, lastID},
				Count:   100,
				Block:   time.Second,
			}).Result()

			if err != nil {
				if err == redis.Nil {
					continue
				}
				// Context canceled or other error
				if ctx.Err() != nil {
					return
				}
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					data, _ := msg.Values["data"].(string)
					event := storage.Event{
						ID:    msg.ID,
						Topic: topic,
						Data:  data,
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

func (e *EventStorage) Close() error {
	return e.client.Close()
}

func streamKey(topic string) string { return "events:" + topic }
