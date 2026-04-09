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
	ErrStreamNotFound   = errors.New("stream definition not found")
	ErrStreamValidation = errors.New("stream data validation failed")
)

// StreamKey uniquely identifies a stream.
type StreamKey struct {
	Group   string
	Version string
	Kind    string
	Name    string
}

// String returns the full key as group/version/kind/name.
func (k StreamKey) String() string {
	return fmt.Sprintf("%s/%s/%s/%s", k.Group, k.Version, k.Kind, k.Name)
}

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
func (s *StreamService) Append(ctx context.Context, key StreamKey, ts time.Time, data map[string]any) error {
	def, err := s.getDefinition(ctx, key)
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

	streamKey := key.String()
	s.logger.Debug("appending to stream", "key", streamKey, "timestamp", ts)

	return s.streams.Add(ctx, streamKey, ts, string(jsonData))
}

// Latest gets the most recent entry from the stream.
func (s *StreamService) Latest(ctx context.Context, key StreamKey) (*storage.StreamEntry, error) {
	_, err := s.getDefinition(ctx, key)
	if err != nil {
		return nil, err
	}

	streamKey := key.String()
	s.logger.Debug("getting latest from stream", "key", streamKey)

	return s.streams.Latest(ctx, streamKey)
}

// Range gets entries within a time range.
func (s *StreamService) Range(ctx context.Context, key StreamKey, from, to time.Time) ([]*storage.StreamEntry, error) {
	_, err := s.getDefinition(ctx, key)
	if err != nil {
		return nil, err
	}

	streamKey := key.String()
	s.logger.Debug("getting range from stream", "key", streamKey, "from", from, "to", to)

	return s.streams.Range(ctx, streamKey, from, to)
}

// Watch subscribes to new entries on the stream.
func (s *StreamService) Watch(ctx context.Context, key StreamKey) (<-chan storage.WatchEvent, error) {
	_, err := s.getDefinition(ctx, key)
	if err != nil {
		return nil, err
	}

	streamKey := key.String()
	s.logger.Debug("watching stream", "key", streamKey)

	return s.streams.Watch(ctx, streamKey)
}

// getDefinition retrieves the StreamDefinition by key.
func (s *StreamService) getDefinition(ctx context.Context, key StreamKey) (*StreamDefinitionSpec, error) {
	// StreamDefinition is stored with name as the identifier
	// We need to find the one that matches group/version/kind
	row, err := s.rows.Get(ctx, storage.Key{
		Type:       "core/v1/StreamDefinition",
		Tradespace: "default",
		Name:       key.Name,
	})
	if err != nil {
		s.logger.Warn("stream definition not found", "key", key.String(), "error", err)
		return nil, fmt.Errorf("%w: %s", ErrStreamNotFound, key.String())
	}

	var spec StreamDefinitionSpec
	if err := json.Unmarshal([]byte(row.Data), &spec); err != nil {
		return nil, fmt.Errorf("invalid stream definition data: %w", err)
	}

	// Verify the spec matches the requested key
	if spec.Group != key.Group || spec.Version != key.Version || spec.Kind != key.Kind {
		s.logger.Warn("stream definition mismatch",
			"requested", key.String(),
			"found", fmt.Sprintf("%s/%s/%s", spec.Group, spec.Version, spec.Kind))
		return nil, fmt.Errorf("%w: %s", ErrStreamNotFound, key.String())
	}

	return &spec, nil
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
