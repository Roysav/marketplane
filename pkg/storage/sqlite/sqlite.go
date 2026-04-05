// Package sqlite provides a SQLite record storage implementation.
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
	ErrNotFound      = errors.New("record not found")
	ErrAlreadyExists = errors.New("record already exists")
)

// Storage implements storage.RecordStorage using SQLite.
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
	CREATE TABLE IF NOT EXISTS records (
		type TEXT NOT NULL,
		tradespace TEXT NOT NULL,
		name TEXT NOT NULL,
		labels TEXT,
		data TEXT,
		resource_version INTEGER NOT NULL DEFAULT 1,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		PRIMARY KEY (type, tradespace, name)
	);

	CREATE INDEX IF NOT EXISTS idx_records_type ON records(type);
	CREATE INDEX IF NOT EXISTS idx_records_tradespace ON records(tradespace);
	`
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

func (s *Storage) Create(ctx context.Context, r *storage.Record) (*storage.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	labelsJSON, _ := json.Marshal(r.Labels)

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO records (type, tradespace, name, labels, data, resource_version, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, 1, ?, ?)
	`, r.Type, r.Tradespace, r.Name, labelsJSON, r.Data, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))

	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return nil, fmt.Errorf("%w: %s %s/%s", ErrAlreadyExists, r.Type, r.Tradespace, r.Name)
		}
		return nil, fmt.Errorf("failed to create record: %w", err)
	}

	return &storage.Record{
		Type:            r.Type,
		Tradespace:      r.Tradespace,
		Name:            r.Name,
		Labels:          r.Labels,
		Data:            r.Data,
		ResourceVersion: 1,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, nil
}

func (s *Storage) Get(ctx context.Context, key storage.Key) (*storage.Record, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT type, tradespace, name, labels, data, resource_version, created_at, updated_at
		FROM records
		WHERE type = ? AND tradespace = ? AND name = ?
	`, key.Type, key.Tradespace, key.Name)

	return scanRecord(row)
}

func (s *Storage) Update(ctx context.Context, r *storage.Record) (*storage.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	labelsJSON, _ := json.Marshal(r.Labels)

	result, err := s.db.ExecContext(ctx, `
		UPDATE records
		SET labels = ?, data = ?, resource_version = resource_version + 1, updated_at = ?
		WHERE type = ? AND tradespace = ? AND name = ?
	`, labelsJSON, r.Data, now.Format(time.RFC3339Nano), r.Type, r.Tradespace, r.Name)

	if err != nil {
		return nil, fmt.Errorf("failed to update record: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return nil, ErrNotFound
	}

	return s.Get(ctx, r.Key())
}

func (s *Storage) Delete(ctx context.Context, key storage.Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	result, err := s.db.ExecContext(ctx, `
		DELETE FROM records
		WHERE type = ? AND tradespace = ? AND name = ?
	`, key.Type, key.Tradespace, key.Name)

	if err != nil {
		return fmt.Errorf("failed to delete record: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrNotFound
	}

	return nil
}

func (s *Storage) List(ctx context.Context, q storage.Query) ([]*storage.Record, error) {
	query := `
		SELECT type, tradespace, name, labels, data, resource_version, created_at, updated_at
		FROM records
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

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list records: %w", err)
	}
	defer rows.Close()

	var records []*storage.Record
	for rows.Next() {
		r, err := scanRecordRows(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}

	return records, rows.Err()
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func scanRecord(row *sql.Row) (*storage.Record, error) {
	var (
		typ             string
		tradespace      string
		name            string
		labelsJSON      sql.NullString
		data            sql.NullString
		resourceVersion int64
		createdAt       string
		updatedAt       string
	)

	err := row.Scan(&typ, &tradespace, &name, &labelsJSON, &data, &resourceVersion, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("failed to scan record: %w", err)
	}

	return buildRecord(typ, tradespace, name, labelsJSON, data, resourceVersion, createdAt, updatedAt)
}

func scanRecordRows(rows *sql.Rows) (*storage.Record, error) {
	var (
		typ             string
		tradespace      string
		name            string
		labelsJSON      sql.NullString
		data            sql.NullString
		resourceVersion int64
		createdAt       string
		updatedAt       string
	)

	err := rows.Scan(&typ, &tradespace, &name, &labelsJSON, &data, &resourceVersion, &createdAt, &updatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to scan record: %w", err)
	}

	return buildRecord(typ, tradespace, name, labelsJSON, data, resourceVersion, createdAt, updatedAt)
}

func buildRecord(typ, tradespace, name string, labelsJSON, data sql.NullString, resourceVersion int64, createdAt, updatedAt string) (*storage.Record, error) {
	var labels map[string]string
	if labelsJSON.Valid {
		json.Unmarshal([]byte(labelsJSON.String), &labels)
	}

	created, _ := time.Parse(time.RFC3339Nano, createdAt)
	updated, _ := time.Parse(time.RFC3339Nano, updatedAt)

	dataStr := ""
	if data.Valid {
		dataStr = data.String
	}

	return &storage.Record{
		Type:            typ,
		Tradespace:      tradespace,
		Name:            name,
		Labels:          labels,
		Data:            dataStr,
		ResourceVersion: resourceVersion,
		CreatedAt:       created,
		UpdatedAt:       updated,
	}, nil
}
