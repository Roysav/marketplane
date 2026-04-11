// Package sqlite provides a SQLite row storage implementation.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/roysav/marketplane/pkg/storage"
)

var (
	ErrNotFound      = errors.New("row not found")
	ErrAlreadyExists = errors.New("row already exists")
)

// Storage implements storage.RowStorage using SQLite.
type Storage struct {
	db *sql.DB
	mu sync.Mutex
}

// New creates a new SQLite storage.
func New(ctx context.Context, dsn string) (*Storage, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return nil, err
	}
	if dsn != ":memory:" {
		if _, err := db.ExecContext(ctx, "PRAGMA journal_mode = WAL"); err != nil {
			return nil, err
		}
	}

	s := &Storage{db: db}
	if err := s.migrate(ctx); err != nil {
		return nil, fmt.Errorf("migration failed: %w", err)
	}

	return s, nil
}

func (s *Storage) migrate(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS rows (
		type TEXT NOT NULL,
		tradespace TEXT NOT NULL,
		name TEXT NOT NULL,
		labels TEXT,
		data TEXT,
		resource_version INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		finalizers TEXT NOT NULL DEFAULT '[]',
		deletion_timestamp TEXT,
		PRIMARY KEY (type, tradespace, name)
	);

	CREATE INDEX IF NOT EXISTS idx_rows_type ON rows(type);
	CREATE INDEX IF NOT EXISTS idx_rows_tradespace ON rows(tradespace);
	`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return err
	}
	// Add columns for pre-existing databases.
	// modernc.org/sqlite does not support "ADD COLUMN IF NOT EXISTS", so we attempt
	// the ALTER and ignore "duplicate column" / "already exists" errors.
	// modernc.org/sqlite returns "duplicate column name: <col>" for this condition;
	// we match case-insensitively to be robust across driver versions.
	for _, stmt := range []string{
		`ALTER TABLE rows ADD COLUMN finalizers TEXT NOT NULL DEFAULT '[]'`,
		`ALTER TABLE rows ADD COLUMN deletion_timestamp TEXT`,
	} {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			msg := strings.ToLower(err.Error())
			if strings.Contains(msg, "duplicate column") || strings.Contains(msg, "already exists") {
				continue
			}
			return err
		}
	}
	return nil
}

func (s *Storage) Create(ctx context.Context, r *storage.Row) (*storage.Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	labelsJSON, _ := json.Marshal(r.Labels)
	finalizersJSON, _ := json.Marshal(r.Finalizers)

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO rows (type, tradespace, name, labels, data, resource_version, created_at, updated_at, finalizers, deletion_timestamp)
		VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
	`, r.Type, r.Tradespace, r.Name, labelsJSON, r.Data, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano),
		finalizersJSON, nil)

	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return nil, fmt.Errorf("%w: %s %s/%s", ErrAlreadyExists, r.Type, r.Tradespace, r.Name)
		}
		return nil, fmt.Errorf("failed to create row: %w", err)
	}

	return &storage.Row{
		Type:            r.Type,
		Tradespace:      r.Tradespace,
		Name:            r.Name,
		Labels:          r.Labels,
		Data:            r.Data,
		ResourceVersion: 1,
		CreatedAt:       now,
		UpdatedAt:       now,
		Finalizers:      r.Finalizers,
	}, nil
}

func (s *Storage) Get(ctx context.Context, key storage.Key) (*storage.Row, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT type, tradespace, name, labels, data, resource_version, created_at, updated_at, finalizers, deletion_timestamp
		FROM rows
		WHERE type = ? AND tradespace = ? AND name = ?
	`, key.Type, key.Tradespace, key.Name)

	return scanRow(row)
}

func (s *Storage) Update(ctx context.Context, r *storage.Row) (*storage.Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	labelsJSON, _ := json.Marshal(r.Labels)
	finalizersJSON, _ := json.Marshal(r.Finalizers)

	var deletionTimestamp *string
	if r.DeletionTimestamp != nil {
		ts := r.DeletionTimestamp.UTC().Format(time.RFC3339Nano)
		deletionTimestamp = &ts
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE rows
		SET labels = ?, data = ?, resource_version = resource_version + 1, updated_at = ?,
		    finalizers = ?, deletion_timestamp = ?
		WHERE type = ? AND tradespace = ? AND name = ? AND resource_version = ?
	`, labelsJSON, r.Data, now.Format(time.RFC3339Nano),
		finalizersJSON, deletionTimestamp,
		r.Type, r.Tradespace, r.Name, r.ResourceVersion)

	if err != nil {
		return nil, fmt.Errorf("failed to update row: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		var exists bool
		s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM rows WHERE type = ? AND tradespace = ? AND name = ?)`,
			r.Type, r.Tradespace, r.Name).Scan(&exists)
		if !exists {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("conflict: resource version mismatch")
	}

	return s.Get(ctx, r.Key())
}

func (s *Storage) Delete(ctx context.Context, key storage.Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	result, err := s.db.ExecContext(ctx, `
		DELETE FROM rows
		WHERE type = ? AND tradespace = ? AND name = ?
	`, key.Type, key.Tradespace, key.Name)

	if err != nil {
		return fmt.Errorf("failed to delete row: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return ErrNotFound
	}

	return nil
}

func (s *Storage) List(ctx context.Context, q storage.Query) ([]*storage.Row, error) {
	query := `
		SELECT type, tradespace, name, labels, data, resource_version, created_at, updated_at, finalizers, deletion_timestamp
		FROM rows
		WHERE type = ?
	`
	args := []any{q.Type}

	if q.Tradespace != "" {
		query += " AND tradespace = ?"
		args = append(args, q.Tradespace)
	}

	for k, v := range q.Labels {
		query += " AND json_extract(labels, ?) = ?"
		args = append(args, "$."+k, v)
	}

	query += " ORDER BY created_at ASC"

	if q.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", q.Limit)
	}

	dbRows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list rows: %w", err)
	}
	defer dbRows.Close()

	var result []*storage.Row
	for dbRows.Next() {
		r, err := scanRows(dbRows)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}

	return result, dbRows.Err()
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func scanRow(row *sql.Row) (*storage.Row, error) {
	var (
		typ               string
		tradespace        string
		name              string
		labelsJSON        sql.NullString
		data              sql.NullString
		resourceVersion   int64
		createdAt         string
		updatedAt         string
		finalizersJSON    sql.NullString
		deletionTimestamp sql.NullString
	)

	err := row.Scan(&typ, &tradespace, &name, &labelsJSON, &data, &resourceVersion, &createdAt, &updatedAt, &finalizersJSON, &deletionTimestamp)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	return buildRow(typ, tradespace, name, labelsJSON, data, resourceVersion, createdAt, updatedAt, finalizersJSON, deletionTimestamp)
}

func scanRows(rows *sql.Rows) (*storage.Row, error) {
	var (
		typ               string
		tradespace        string
		name              string
		labelsJSON        sql.NullString
		data              sql.NullString
		resourceVersion   int64
		createdAt         string
		updatedAt         string
		finalizersJSON    sql.NullString
		deletionTimestamp sql.NullString
	)

	err := rows.Scan(&typ, &tradespace, &name, &labelsJSON, &data, &resourceVersion, &createdAt, &updatedAt, &finalizersJSON, &deletionTimestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	return buildRow(typ, tradespace, name, labelsJSON, data, resourceVersion, createdAt, updatedAt, finalizersJSON, deletionTimestamp)
}

func buildRow(typ, tradespace, name string, labelsJSON, data sql.NullString, resourceVersion int64, createdAt, updatedAt string, finalizersJSON, deletionTimestamp sql.NullString) (*storage.Row, error) {
	var labels map[string]string
	if labelsJSON.Valid {
		json.Unmarshal([]byte(labelsJSON.String), &labels)
	}

	var finalizers []string
	if finalizersJSON.Valid && finalizersJSON.String != "" {
		json.Unmarshal([]byte(finalizersJSON.String), &finalizers)
	}

	var deletionTS *time.Time
	if deletionTimestamp.Valid && deletionTimestamp.String != "" {
		t, _ := time.Parse(time.RFC3339Nano, deletionTimestamp.String)
		deletionTS = &t
	}

	created, _ := time.Parse(time.RFC3339Nano, createdAt)
	updated, _ := time.Parse(time.RFC3339Nano, updatedAt)

	dataStr := ""
	if data.Valid {
		dataStr = data.String
	}

	return &storage.Row{
		Type:              typ,
		Tradespace:        tradespace,
		Name:              name,
		Labels:            labels,
		Data:              dataStr,
		ResourceVersion:   resourceVersion,
		CreatedAt:         created,
		UpdatedAt:         updated,
		Finalizers:        finalizers,
		DeletionTimestamp: deletionTS,
	}, nil
}
