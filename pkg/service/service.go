// Package service provides the business logic layer for managing records.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/storage"
	"github.com/roysav/marketplane/pkg/validator"
)

var (
	ErrNotFound      = errors.New("record not found")
	ErrAlreadyExists = errors.New("record already exists")
	ErrConflict      = errors.New("record conflict")
	ErrValidation    = errors.New("validation failed")
	ErrInvalidScope  = errors.New("invalid scope")
	ErrTerminating   = errors.New("record is terminating")
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
	if len(r.Status) > 0 {
		return nil, fmt.Errorf("%w: status cannot be set on create", ErrValidation)
	}

	// Validate scope (tradespace vs global)
	if err := s.validator.ValidateScope(ctx, r); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidScope, err)
	}

	// Validate the record spec against schema
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
	// Fetch current state to enforce finalizer rules.
	current, err := s.Get(ctx, r.TypeMeta.GVK().Type(), r.ObjectMeta.Tradespace, r.ObjectMeta.Name)
	if err != nil {
		return nil, err
	}
	// Reject adding finalizers to a terminating record.
	if current.ObjectMeta.IsTerminating() {
		added := setDiff(r.ObjectMeta.Finalizers, current.ObjectMeta.Finalizers)
		if len(added) > 0 {
			return nil, fmt.Errorf("%w: cannot add finalizers to a terminating record: %v", ErrTerminating, added)
		}
	}

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
	persistedRow, err := s.rows.Update(ctx, row)
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("%w: %s/%s", ErrNotFound, r.ObjectMeta.Tradespace, r.ObjectMeta.Name)
		}
		if isConflict(err) {
			return nil, fmt.Errorf("%w: %s/%s", ErrConflict, r.ObjectMeta.Tradespace, r.ObjectMeta.Name)
		}
		return nil, err
	}

	updated, err := rowToRecord(persistedRow)
	if err != nil {
		return nil, err
	}

	s.publishEvent(ctx, "updated", updated)

	// Auto-trigger hard delete when the last finalizer is removed from a terminating record.
	if updated.ObjectMeta.IsTerminating() && len(updated.ObjectMeta.Finalizers) == 0 {
		if err := s.hardDelete(ctx, updated); err != nil {
			return nil, err
		}
		return updated, nil
	}

	return updated, nil
}

// Delete removes a record. If the record has finalizers, it is soft-deleted
// (DeletionTimestamp set) and a "terminating" event is published. The hard
// delete happens automatically when the last finalizer is removed via Update.
func (s *Service) Delete(ctx context.Context, typeStr, tradespace, name string) error {
	current, err := s.Get(ctx, typeStr, tradespace, name)
	if err != nil {
		return err
	}

	// Already terminating — idempotent.
	if current.ObjectMeta.IsTerminating() {
		return nil
	}

	// Atomically mark as terminating via optimistic-lock Update.
	// This closes the race with a concurrent Update that might add a finalizer:
	// if another writer bumped the ResourceVersion between our Get and this
	// Update, the Update will fail with a conflict and the caller should retry.
	now := time.Now().UTC()
	current.ObjectMeta.DeletionTimestamp = &now

	row, err := recordToRow(current)
	if err != nil {
		return err
	}

	persisted, err := s.rows.Update(ctx, row)
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("%w: %s %s/%s", ErrNotFound, typeStr, tradespace, name)
		}
		return err
	}

	// Optimistic lock succeeded — finalizer set is now frozen.
	// Hard-delete immediately when there are no finalizers.
	if len(persisted.Finalizers) == 0 {
		return s.hardDelete(ctx, current)
	}

	s.publishEvent(ctx, "terminating", current)
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

func (s *Service) hardDelete(ctx context.Context, r *record.Record) error {
	err := s.rows.Delete(ctx, storage.Key{
		Type:       r.TypeMeta.GVK().Type(),
		Tradespace: r.ObjectMeta.Tradespace,
		Name:       r.ObjectMeta.Name,
	})
	if err != nil && !isNotFound(err) {
		return err
	}
	s.publishEvent(ctx, "deleted", r)
	return nil
}

// setDiff returns elements in a that are not in b.
func setDiff(a, b []string) []string {
	set := make(map[string]struct{}, len(b))
	for _, v := range b {
		set[v] = struct{}{}
	}
	var diff []string
	for _, v := range a {
		if _, ok := set[v]; !ok {
			diff = append(diff, v)
		}
	}
	return diff
}

func recordToRow(r *record.Record) (*storage.Row, error) {
	data, err := json.Marshal(map[string]any{
		"spec":   r.Spec,
		"status": r.Status,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal record data: %w", err)
	}
	tradespace := r.ObjectMeta.Tradespace
	if tradespace == "" {
		tradespace = "default"
	}
	return &storage.Row{
		Type:              r.TypeMeta.GVK().Type(),
		Tradespace:        tradespace,
		Name:              r.ObjectMeta.Name,
		Labels:            r.ObjectMeta.Labels,
		Data:              string(data),
		ResourceVersion:   r.ObjectMeta.ResourceVersion,
		Finalizers:        r.ObjectMeta.Finalizers,
		DeletionTimestamp: r.ObjectMeta.DeletionTimestamp,
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
			Tradespace:        row.Tradespace,
			Name:              row.Name,
			Labels:            row.Labels,
			ResourceVersion:   row.ResourceVersion,
			CreatedAt:         row.CreatedAt,
			UpdatedAt:         row.UpdatedAt,
			Finalizers:        row.Finalizers,
			DeletionTimestamp: row.DeletionTimestamp,
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

func isConflict(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, storage.ErrConflict) ||
		strings.Contains(err.Error(), "conflict")
}
