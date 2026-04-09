// Package redis provides Redis-backed storage implementations.
package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Options for connecting to Redis.
type Options struct {
	Addr     string // e.g., "localhost:6379"
	Password string
	DB       int
}

// NewClient creates a Redis client and verifies connectivity.
func NewClient(ctx context.Context, opts Options) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     opts.Addr,
		Password: opts.Password,
		DB:       opts.DB,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return client, nil
}
