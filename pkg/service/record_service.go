// Package service provides the business logic layer for managing records.
package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/storage"
)

var (
	ErrNotFound      = errors.New("record not found")
	ErrAlreadyExists = errors.New("record already exists")
	ErrValidation    = errors.New("validation failed")
	ErrInvalidScope  = errors.New("invalid scope")
)

const DefaultMetaRecordSyncInterval = 30 * time.Second

// Config holds configuration for the RecordService.
// TODO: Remove the config thing. Just pass it directly to constructor of the RecordService, remove the default slog, we already pass one from main.go.
type Config struct {
	Rows      storage.RowStorage
	Events    storage.EventStorage
	Validator *record.Validator
	Logger    *slog.Logger // optional, defaults to slog.Default()
}

// RecordService provides operations on records.
// TODO: Use either a ptr for rows, events, validator. or regular ahhh forgot.
type RecordService struct {
	rows      storage.RowStorage
	events    storage.EventStorage
	validator *record.Validator
	logger    *slog.Logger
}

// New creates a new RecordService.
func New(cfg Config) *RecordService {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &RecordService{
		rows:      cfg.Rows,
		events:    cfg.Events,
		validator: cfg.Validator,
		logger:    logger.With("component", "service"),
	}
}

// Create creates a new record.
func (s *RecordService) Create(ctx context.Context, r *record.Record) (*record.Record, error) {
	// Validate the record spec against schema
	if err := s.validator.Validate(r); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrValidation, err)
	}
	if err := s.validateMetaRecordCreate(r); err != nil {
		return nil, err
	}

	// Convert to row
	row, err := r.ToRow()
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
	if err := s.registerCreatedMetaRecord(ctx, r); err != nil {
		return nil, err
	}

	// Publish event
	s.publishEvent(ctx, "created", r)

	return record.RecordFromRow(created)
}

// Get retrieves a record by key.
func (s *RecordService) Get(ctx context.Context, typeStr, tradespace, name string) (*record.Record, error) {
	row, err := s.rows.Get(ctx, record.Key(typeStr, tradespace, name))
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("%w: %s %s/%s", ErrNotFound, typeStr, tradespace, name)
		}
		return nil, err
	}

	return record.RecordFromRow(row)
}

// Update updates an existing record.
func (s *RecordService) Update(ctx context.Context, r *record.Record, lastApplied []byte) (*record.Record, error) {
	if err := s.validateMetaRecordUpdate(ctx, r); err != nil {
		return nil, err
	}
	if err := s.validator.Validate(r); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrValidation, err)
	}

	row, err := r.ToRow()
	if err != nil {
		return nil, err
	}

	row, err = s.rows.Update(ctx, row, lastApplied)
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("%w: %s/%s", ErrNotFound, r.ObjectMeta.Tradespace, r.ObjectMeta.Name)
		}
		return nil, err
	}
	if err := s.registerMetaRecord(r); err != nil {
		return nil, err
	}

	// Publish event
	s.publishEvent(ctx, "updated", r)

	return record.RecordFromRow(row)
}

// Delete removes a record.
func (s *RecordService) Delete(ctx context.Context, typeStr, tradespace, name string) error {
	err := s.rows.Delete(ctx, record.Key(typeStr, tradespace, name))
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("%w: %s %s/%s", ErrNotFound, typeStr, tradespace, name)
		}
		return err
	}

	// Publish event
	s.publishEvent(ctx, "deleted", &record.Record{
		Type:       typeStr,
		ObjectMeta: record.ObjectMeta{Tradespace: tradespace, Name: name},
	})

	return nil
}

// List returns records matching the query. If tradespace is empty, all
// tradespaces for the given type are returned.
func (s *RecordService) List(ctx context.Context, typeStr string, tradespace string, labels map[string]string) ([]*record.Record, error) {
	prefix := record.Key(typeStr, tradespace, "")
	if tradespace == "" {
		prefix = typeStr + "/"
	}
	rows, err := s.rows.List(ctx, storage.Query{
		Prefix: prefix,
		Labels: labels,
		Limit:  0,
	},
	)
	if err != nil {
		return nil, err
	}

	records := make([]*record.Record, 0, len(rows))
	for _, row := range rows {
		r, err := record.RecordFromRow(row)
		if err != nil {
			s.logger.Warn("skipping row: conversion failed",
				"row", row,
				"error", err,
			)
			continue
		}
		records = append(records, r)
	}

	return records, nil
}

// Watch returns a channel of events for records of the given type.
func (s *RecordService) Watch(ctx context.Context, typeStr string) (<-chan storage.Event, error) {
	if s.events == nil {
		return nil, errors.New("event storage not configured")
	}
	topic := eventTopic(typeStr)
	return s.events.Subscribe(ctx, topic)
}

func (s *RecordService) StartMetaRecordSync(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultMetaRecordSyncInterval
	}

	if err := s.SyncMetaRecords(ctx); err != nil {
		s.logger.Warn("failed to sync MetaRecord schemas", "error", err)
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.SyncMetaRecords(ctx); err != nil {
					s.logger.Warn("failed to sync MetaRecord schemas", "error", err)
				}
			}
		}
	}()
}

func (s *RecordService) SyncMetaRecords(ctx context.Context) error {
	metaRecords, err := s.List(ctx, record.MetaRecordType, "", nil)
	if err != nil {
		return fmt.Errorf("list MetaRecords: %w", err)
	}

	sort.Slice(metaRecords, func(i, j int) bool {
		return metaRecords[i].Key() < metaRecords[j].Key()
	})

	var syncErrs []error
	for _, metaRecord := range metaRecords {
		if err := s.registerMetaRecord(metaRecord); err != nil {
			syncErrs = append(syncErrs, fmt.Errorf("%s: %w", metaRecord.Key(), err))
		}
	}

	return errors.Join(syncErrs...)
}

func (s *RecordService) publishEvent(ctx context.Context, action string, r *record.Record) {
	if s.events == nil {
		return
	}

	event := map[string]any{
		"action":     action,
		"type":       r.Type,
		"tradespace": r.ObjectMeta.Tradespace,
		"name":       r.ObjectMeta.Name,
	}
	data, _ := json.Marshal(event)
	topic := eventTopic(r.Type)

	_, err := s.events.Publish(ctx, topic, string(data))
	if err != nil {
		s.logger.Warn("failed to publish event",
			"action", action,
			"type", r.Type,
			"tradespace", r.ObjectMeta.Tradespace,
			"name", r.ObjectMeta.Name,
			"error", err,
		)
	} else {
		s.logger.Debug("published event",
			"action", action,
			"type", r.Type,
			"tradespace", r.ObjectMeta.Tradespace,
			"name", r.ObjectMeta.Name,
		)
	}
}

func eventTopic(typeStr string) string {
	return "record:" + typeStr
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

func (s *RecordService) validateMetaRecordCreate(r *record.Record) error {
	if r.Type != record.MetaRecordType {
		return nil
	}

	if err := s.validator.CheckMetaRecord(r); err != nil {
		return fmt.Errorf("%w: %v", ErrValidation, err)
	}

	return nil
}

func (s *RecordService) validateMetaRecordUpdate(ctx context.Context, r *record.Record) error {
	if r.Type != record.MetaRecordType {
		return nil
	}

	current, err := s.Get(ctx, r.Type, r.ObjectMeta.Tradespace, r.ObjectMeta.Name)
	if err != nil {
		return err
	}

	if err := record.EnsureMetaRecordDefinitionImmutable(current, r); err != nil {
		return fmt.Errorf("%w: %v", ErrValidation, err)
	}
	if err := s.validator.CheckMetaRecord(r); err != nil {
		return fmt.Errorf("%w: %v", ErrValidation, err)
	}

	return nil
}

func (s *RecordService) registerCreatedMetaRecord(ctx context.Context, r *record.Record) error {
	if err := s.registerMetaRecord(r); err != nil {
		if r.Type == record.MetaRecordType {
			if deleteErr := s.rows.Delete(ctx, r.Key()); deleteErr != nil {
				s.logger.Warn("failed to roll back MetaRecord after validator registration failure",
					"key", r.Key(),
					"error", deleteErr,
				)
			}
		}
		return err
	}

	return nil
}

func (s *RecordService) registerMetaRecord(r *record.Record) error {
	if r.Type != record.MetaRecordType {
		return nil
	}

	if err := s.validator.RegisterMetaRecord(r); err != nil {
		return fmt.Errorf("%w: %v", ErrValidation, err)
	}

	return nil
}
