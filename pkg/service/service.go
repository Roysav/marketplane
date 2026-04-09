// Package service provides the business logic layer for managing records.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/storage"
	"github.com/roysav/marketplane/pkg/validator"
)

var (
	ErrNotFound      = errors.New("record not found")
	ErrAlreadyExists = errors.New("record already exists")
	ErrValidation    = errors.New("validation failed")
)

// Config holds configuration for the Service.
type Config struct {
	Rows   storage.RowStorage
	Events storage.EventStorage // optional, enables watch/events
	Logger *slog.Logger         // optional, defaults to slog.Default()
}

// Service provides operations on records.
type Service struct {
	rows      storage.RowStorage
	events    storage.EventStorage
	validator *validator.Validator
	logger    *slog.Logger
}

// New creates a new Service.
func New(cfg Config) *Service {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Service{
		rows:      cfg.Rows,
		events:    cfg.Events,
		validator: validator.New(cfg.Rows),
		logger:    logger.With("component", "service"),
	}
}

// Create creates a new record.
func (s *Service) Create(ctx context.Context, r *record.Record) (*record.Record, error) {
	// Validate the record
	if err := s.validator.Validate(ctx, r); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrValidation, err)
	}

	// Convert to row
	row, err := recordToRow(r)
	if err != nil {
		return nil, err
	}

	// Store
	created, err := s.rows.Create(ctx, row)
	if err != nil {
		if isAlreadyExists(err) {
			return nil, fmt.Errorf("%w: %s/%s", ErrAlreadyExists, r.ObjectMeta.Tradespace, r.ObjectMeta.Name)
		}
		return nil, err
	}

	// Publish event
	s.publishEvent(ctx, "created", r)

	return rowToRecord(created)
}

// Get retrieves a record by key.
func (s *Service) Get(ctx context.Context, typeStr, tradespace, name string) (*record.Record, error) {
	row, err := s.rows.Get(ctx, storage.Key{
		Type:       typeStr,
		Tradespace: tradespace,
		Name:       name,
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("%w: %s %s/%s", ErrNotFound, typeStr, tradespace, name)
		}
		return nil, err
	}

	return rowToRecord(row)
}

// Update updates an existing record.
func (s *Service) Update(ctx context.Context, r *record.Record) (*record.Record, error) {
	// Validate the record
	if err := s.validator.Validate(ctx, r); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrValidation, err)
	}

	// Convert to row
	row, err := recordToRow(r)
	if err != nil {
		return nil, err
	}

	// Update
	updated, err := s.rows.Update(ctx, row)
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("%w: %s/%s", ErrNotFound, r.ObjectMeta.Tradespace, r.ObjectMeta.Name)
		}
		return nil, err
	}

	// Publish event
	s.publishEvent(ctx, "updated", r)

	return rowToRecord(updated)
}

// Delete removes a record.
func (s *Service) Delete(ctx context.Context, typeStr, tradespace, name string) error {
	err := s.rows.Delete(ctx, storage.Key{
		Type:       typeStr,
		Tradespace: tradespace,
		Name:       name,
	})
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("%w: %s %s/%s", ErrNotFound, typeStr, tradespace, name)
		}
		return err
	}

	// Publish event
	s.publishEvent(ctx, "deleted", &record.Record{
		TypeMeta:   record.TypeMetaFromType(typeStr),
		ObjectMeta: record.ObjectMeta{Tradespace: tradespace, Name: name},
	})

	return nil
}

// List returns records matching the query.
func (s *Service) List(ctx context.Context, typeStr string, tradespace string, labels map[string]string) ([]*record.Record, error) {
	rows, err := s.rows.List(ctx, storage.Query{
		Type:       typeStr,
		Tradespace: tradespace,
		Labels:     labels,
	})
	if err != nil {
		return nil, err
	}

	records := make([]*record.Record, 0, len(rows))
	for _, row := range rows {
		r, err := rowToRecord(row)
		if err != nil {
			s.logger.Warn("skipping row: conversion failed",
				"type", row.Type,
				"tradespace", row.Tradespace,
				"name", row.Name,
				"error", err,
			)
			continue
		}
		records = append(records, r)
	}

	return records, nil
}

// Watch returns a channel of events for records of the given type.
func (s *Service) Watch(ctx context.Context, typeStr string) (<-chan storage.Event, error) {
	if s.events == nil {
		return nil, errors.New("event storage not configured")
	}
	topic := eventTopic(typeStr)
	return s.events.Subscribe(ctx, topic)
}

func (s *Service) publishEvent(ctx context.Context, action string, r *record.Record) {
	if s.events == nil {
		return
	}

	typeStr := r.TypeMeta.GVK().Type()
	event := map[string]any{
		"action":     action,
		"type":       typeStr,
		"tradespace": r.ObjectMeta.Tradespace,
		"name":       r.ObjectMeta.Name,
	}
	data, _ := json.Marshal(event)
	topic := eventTopic(typeStr)

	_, err := s.events.Publish(ctx, topic, string(data))
	if err != nil {
		s.logger.Warn("failed to publish event",
			"action", action,
			"type", typeStr,
			"tradespace", r.ObjectMeta.Tradespace,
			"name", r.ObjectMeta.Name,
			"error", err,
		)
	} else {
		s.logger.Debug("published event",
			"action", action,
			"type", typeStr,
			"tradespace", r.ObjectMeta.Tradespace,
			"name", r.ObjectMeta.Name,
		)
	}
}

func eventTopic(typeStr string) string {
	return "record:" + typeStr
}

func recordToRow(r *record.Record) (*storage.Row, error) {
	data, err := json.Marshal(map[string]any{
		"spec":   r.Spec,
		"status": r.Status,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal record data: %w", err)
	}

	return &storage.Row{
		Type:            r.TypeMeta.GVK().Type(),
		Tradespace:      r.ObjectMeta.Tradespace,
		Name:            r.ObjectMeta.Name,
		Labels:          r.ObjectMeta.Labels,
		Data:            string(data),
		ResourceVersion: r.ObjectMeta.ResourceVersion,
	}, nil
}

func rowToRecord(row *storage.Row) (*record.Record, error) {
	var data struct {
		Spec   map[string]any `json:"spec"`
		Status map[string]any `json:"status"`
	}
	if row.Data != "" {
		if err := json.Unmarshal([]byte(row.Data), &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal record data: %w", err)
		}
	}

	return &record.Record{
		TypeMeta: record.TypeMetaFromType(row.Type),
		ObjectMeta: record.ObjectMeta{
			Tradespace:      row.Tradespace,
			Name:            row.Name,
			Labels:          row.Labels,
			ResourceVersion: row.ResourceVersion,
			CreatedAt:       row.CreatedAt,
			UpdatedAt:       row.UpdatedAt,
		},
		Spec:   data.Spec,
		Status: data.Status,
	}, nil
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, storage.ErrNotFound) ||
		strings.Contains(err.Error(), "not found")
}

func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, storage.ErrAlreadyExists) ||
		strings.Contains(err.Error(), "already exists")
}
