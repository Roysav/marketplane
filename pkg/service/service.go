// Package service provides the business logic layer for managing entities.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/roysav/marketplane/pkg/entity"
	"github.com/roysav/marketplane/pkg/storage"
	"github.com/roysav/marketplane/pkg/validator"
)

var (
	ErrNotFound      = errors.New("entity not found")
	ErrAlreadyExists = errors.New("entity already exists")
	ErrValidation    = errors.New("validation failed")
)

// Config holds configuration for the Service.
type Config struct {
	Records storage.RecordStorage
	Events  storage.EventStorage // optional, enables watch/events
	Logger  *slog.Logger         // optional, defaults to slog.Default()
}

// Service provides operations on entities.
type Service struct {
	records   storage.RecordStorage
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
		records:   cfg.Records,
		events:    cfg.Events,
		validator: validator.New(cfg.Records),
		logger:    logger.With("component", "service"),
	}
}

// Create creates a new entity.
func (s *Service) Create(ctx context.Context, e *entity.Entity) (*entity.Entity, error) {
	// Validate the entity
	if err := s.validator.Validate(ctx, e); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrValidation, err)
	}

	// Convert to record
	record, err := entityToRecord(e)
	if err != nil {
		return nil, err
	}

	// Store
	created, err := s.records.Create(ctx, record)
	if err != nil {
		if isAlreadyExists(err) {
			return nil, fmt.Errorf("%w: %s/%s", ErrAlreadyExists, e.ObjectMeta.Tradespace, e.ObjectMeta.Name)
		}
		return nil, err
	}

	// Publish event
	s.publishEvent(ctx, "created", e)

	return recordToEntity(created)
}

// Get retrieves an entity by key.
func (s *Service) Get(ctx context.Context, typeStr, tradespace, name string) (*entity.Entity, error) {
	record, err := s.records.Get(ctx, storage.Key{
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

	return recordToEntity(record)
}

// Update updates an existing entity.
func (s *Service) Update(ctx context.Context, e *entity.Entity) (*entity.Entity, error) {
	// Validate the entity
	if err := s.validator.Validate(ctx, e); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrValidation, err)
	}

	// Convert to record
	record, err := entityToRecord(e)
	if err != nil {
		return nil, err
	}

	// Update
	updated, err := s.records.Update(ctx, record)
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("%w: %s/%s", ErrNotFound, e.ObjectMeta.Tradespace, e.ObjectMeta.Name)
		}
		return nil, err
	}

	// Publish event
	s.publishEvent(ctx, "updated", e)

	return recordToEntity(updated)
}

// Delete removes an entity.
func (s *Service) Delete(ctx context.Context, typeStr, tradespace, name string) error {
	err := s.records.Delete(ctx, storage.Key{
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
	s.publishEvent(ctx, "deleted", &entity.Entity{
		TypeMeta:   entity.TypeMetaFromType(typeStr),
		ObjectMeta: entity.ObjectMeta{Tradespace: tradespace, Name: name},
	})

	return nil
}

// List returns entities matching the query.
func (s *Service) List(ctx context.Context, typeStr string, tradespace string, labels map[string]string) ([]*entity.Entity, error) {
	records, err := s.records.List(ctx, storage.Query{
		Type:       typeStr,
		Tradespace: tradespace,
		Labels:     labels,
	})
	if err != nil {
		return nil, err
	}

	entities := make([]*entity.Entity, 0, len(records))
	for _, r := range records {
		e, err := recordToEntity(r)
		if err != nil {
			s.logger.Warn("skipping record: conversion failed",
				"type", r.Type,
				"tradespace", r.Tradespace,
				"name", r.Name,
				"error", err,
			)
			continue
		}
		entities = append(entities, e)
	}

	return entities, nil
}

// Watch returns a channel of events for entities of the given type.
func (s *Service) Watch(ctx context.Context, typeStr string) (<-chan storage.Event, error) {
	if s.events == nil {
		return nil, errors.New("event storage not configured")
	}
	topic := eventTopic(typeStr)
	return s.events.Subscribe(ctx, topic)
}

func (s *Service) publishEvent(ctx context.Context, action string, e *entity.Entity) {
	if s.events == nil {
		return
	}

	typeStr := e.TypeMeta.GVK().Type()
	event := map[string]any{
		"action":     action,
		"type":       typeStr,
		"tradespace": e.ObjectMeta.Tradespace,
		"name":       e.ObjectMeta.Name,
	}
	data, _ := json.Marshal(event)
	topic := eventTopic(typeStr)

	_, err := s.events.Publish(ctx, topic, string(data))
	if err != nil {
		s.logger.Warn("failed to publish event",
			"action", action,
			"type", typeStr,
			"tradespace", e.ObjectMeta.Tradespace,
			"name", e.ObjectMeta.Name,
			"error", err,
		)
	} else {
		s.logger.Debug("published event",
			"action", action,
			"type", typeStr,
			"tradespace", e.ObjectMeta.Tradespace,
			"name", e.ObjectMeta.Name,
		)
	}
}

func eventTopic(typeStr string) string {
	return "entity:" + typeStr
}

func entityToRecord(e *entity.Entity) (*storage.Record, error) {
	data, err := json.Marshal(map[string]any{
		"spec":   e.Spec,
		"status": e.Status,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal entity data: %w", err)
	}

	return &storage.Record{
		Type:            e.TypeMeta.GVK().Type(),
		Tradespace:      e.ObjectMeta.Tradespace,
		Name:            e.ObjectMeta.Name,
		Labels:          e.ObjectMeta.Labels,
		Data:            string(data),
		ResourceVersion: e.ObjectMeta.ResourceVersion,
	}, nil
}

func recordToEntity(r *storage.Record) (*entity.Entity, error) {
	var data struct {
		Spec   map[string]any `json:"spec"`
		Status map[string]any `json:"status"`
	}
	if r.Data != "" {
		if err := json.Unmarshal([]byte(r.Data), &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entity data: %w", err)
		}
	}

	return &entity.Entity{
		TypeMeta: entity.TypeMetaFromType(r.Type),
		ObjectMeta: entity.ObjectMeta{
			Tradespace:      r.Tradespace,
			Name:            r.Name,
			Labels:          r.Labels,
			ResourceVersion: r.ResourceVersion,
			CreatedAt:       r.CreatedAt,
			UpdatedAt:       r.UpdatedAt,
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
