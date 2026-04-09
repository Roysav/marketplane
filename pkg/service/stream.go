package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/xeipuuv/gojsonschema"

	"github.com/roysav/marketplane/pkg/storage"
)

var (
	ErrStreamNotFound     = errors.New("stream definition not found")
	ErrStreamValidation   = errors.New("stream data validation failed")
)

// StreamDefinitionSpec represents the spec of a StreamDefinition record.
type StreamDefinitionSpec struct {
	Group     string         `json:"group"`
	Version   string         `json:"version"`
	Kind      string         `json:"kind"`
	Schema    map[string]any `json:"schema,omitempty"`
	Retention string         `json:"retention,omitempty"`
}

// StreamService provides operations on streams defined by StreamDefinitions.
type StreamService struct {
	rows    storage.RowStorage
	streams storage.StreamStorage
	logger  *slog.Logger
}

// StreamServiceConfig holds dependencies for StreamService.
type StreamServiceConfig struct {
	Rows    storage.RowStorage
	Streams storage.StreamStorage
	Logger  *slog.Logger
}

// NewStreamService creates a new StreamService.
func NewStreamService(cfg StreamServiceConfig) *StreamService {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &StreamService{
		rows:    cfg.Rows,
		streams: cfg.Streams,
		logger:  logger,
	}
}

// Append adds a data point to the stream.
func (s *StreamService) Append(ctx context.Context, name string, ts time.Time, data map[string]any) error {
	def, err := s.getDefinition(ctx, name)
	if err != nil {
		return err
	}

	// Validate against schema if present
	if def.Schema != nil {
		if err := s.validateData(def.Schema, data); err != nil {
			return err
		}
	}

	// Serialize data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	key := s.streamKey(def, name)
	s.logger.Debug("appending to stream", "name", name, "key", key, "timestamp", ts)

	return s.streams.Add(ctx, key, ts, string(jsonData))
}

// Latest gets the most recent entry from the stream.
func (s *StreamService) Latest(ctx context.Context, name string) (*storage.StreamEntry, error) {
	def, err := s.getDefinition(ctx, name)
	if err != nil {
		return nil, err
	}

	key := s.streamKey(def, name)
	s.logger.Debug("getting latest from stream", "name", name, "key", key)

	return s.streams.Latest(ctx, key)
}

// Range gets entries within a time range.
func (s *StreamService) Range(ctx context.Context, name string, from, to time.Time) ([]*storage.StreamEntry, error) {
	def, err := s.getDefinition(ctx, name)
	if err != nil {
		return nil, err
	}

	key := s.streamKey(def, name)
	s.logger.Debug("getting range from stream", "name", name, "key", key, "from", from, "to", to)

	return s.streams.Range(ctx, key, from, to)
}

// WatchFilter specifies which streams to watch.
type WatchFilter struct {
	Name  string // Specific StreamDefinition name
	Group string // Filter by group
	Kind  string // Filter by kind
}

// Watch subscribes to new entries on streams matching the filter.
func (s *StreamService) Watch(ctx context.Context, filter WatchFilter) (<-chan storage.WatchEvent, error) {
	// If name is specified, watch single stream
	if filter.Name != "" {
		return s.watchSingle(ctx, filter.Name)
	}

	// Otherwise watch by group/kind pattern
	return s.watchPattern(ctx, filter.Group, filter.Kind)
}

// watchSingle watches a single stream by name.
func (s *StreamService) watchSingle(ctx context.Context, name string) (<-chan storage.WatchEvent, error) {
	def, err := s.getDefinition(ctx, name)
	if err != nil {
		return nil, err
	}

	key := s.streamKey(def, name)
	s.logger.Debug("watching stream", "name", name, "key", key)

	return s.streams.Watch(ctx, key)
}

// watchPattern watches all streams matching group/kind.
func (s *StreamService) watchPattern(ctx context.Context, group, kind string) (<-chan storage.WatchEvent, error) {
	// Find all matching StreamDefinitions
	defs, err := s.listDefinitions(ctx, group, kind)
	if err != nil {
		return nil, err
	}

	if len(defs) == 0 {
		return nil, fmt.Errorf("%w: no streams match group=%s kind=%s", ErrStreamNotFound, group, kind)
	}

	// Create merged channel
	merged := make(chan storage.WatchEvent, 100)

	// Start watching each stream
	for _, def := range defs {
		key := s.streamKey(&def.spec, def.name)
		ch, err := s.streams.Watch(ctx, key)
		if err != nil {
			s.logger.Warn("failed to watch stream", "name", def.name, "error", err)
			continue
		}

		// Forward events to merged channel
		go func(name string, ch <-chan storage.WatchEvent) {
			for event := range ch {
				select {
				case merged <- event:
				case <-ctx.Done():
					return
				}
			}
		}(def.name, ch)
	}

	// Close merged channel when context is done
	go func() {
		<-ctx.Done()
		close(merged)
	}()

	s.logger.Debug("watching streams by pattern", "group", group, "kind", kind, "count", len(defs))

	return merged, nil
}

type streamDefWithName struct {
	name string
	spec StreamDefinitionSpec
}

// listDefinitions returns StreamDefinitions matching group/kind.
func (s *StreamService) listDefinitions(ctx context.Context, group, kind string) ([]streamDefWithName, error) {
	// List all StreamDefinitions
	rows, err := s.rows.List(ctx, storage.Query{
		Type:       "core/v1/StreamDefinition",
		Tradespace: "default",
	})
	if err != nil {
		return nil, err
	}

	var results []streamDefWithName
	for _, row := range rows {
		var spec StreamDefinitionSpec
		if err := json.Unmarshal([]byte(row.Data), &spec); err != nil {
			continue
		}

		// Filter by group/kind
		if group != "" && spec.Group != group {
			continue
		}
		if kind != "" && spec.Kind != kind {
			continue
		}

		results = append(results, streamDefWithName{
			name: row.Name,
			spec: spec,
		})
	}

	return results, nil
}

// getDefinition retrieves the StreamDefinition by name.
func (s *StreamService) getDefinition(ctx context.Context, name string) (*StreamDefinitionSpec, error) {
	row, err := s.rows.Get(ctx, storage.Key{
		Type:       "core/v1/StreamDefinition",
		Tradespace: "default",
		Name:       name,
	})
	if err != nil {
		s.logger.Warn("stream definition not found", "name", name, "error", err)
		return nil, fmt.Errorf("%w: %s", ErrStreamNotFound, name)
	}

	var spec StreamDefinitionSpec
	if err := json.Unmarshal([]byte(row.Data), &spec); err != nil {
		return nil, fmt.Errorf("invalid stream definition data: %w", err)
	}

	return &spec, nil
}

// streamKey derives the Redis stream key from the definition.
func (s *StreamService) streamKey(def *StreamDefinitionSpec, name string) string {
	return fmt.Sprintf("%s/%s/%s/%s", def.Group, def.Version, def.Kind, name)
}

// validateData validates data against a JSON schema.
func (s *StreamService) validateData(schemaData map[string]any, data map[string]any) error {
	schema, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(schemaData))
	if err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}

	result, err := schema.Validate(gojsonschema.NewGoLoader(data))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStreamValidation, err)
	}

	if !result.Valid() {
		var msgs []string
		for _, e := range result.Errors() {
			msgs = append(msgs, e.String())
		}
		return fmt.Errorf("%w: %v", ErrStreamValidation, msgs)
	}

	return nil
}
