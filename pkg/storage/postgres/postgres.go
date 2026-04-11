// Package postgres provides PostgreSQL storage implementations.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/roysav/marketplane/pkg/storage"
)

// RowStorage implements storage.RowStorage using PostgreSQL.
type RowStorage struct {
	db *sql.DB
}

// New creates a new PostgreSQL row storage.
// DSN format: "postgres://user:password@host:port/dbname?sslmode=disable"
func New(ctx context.Context, dsn string) (*RowStorage, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Verify connection
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Run migrations
	if err := Migrate(ctx, db); err != nil {
		return nil, fmt.Errorf("migration failed: %w", err)
	}

	return &RowStorage{db: db}, nil
}

// Create creates a new row.
func (s *RowStorage) Create(ctx context.Context, r *storage.Row) (*storage.Row, error) {
	labels, err := json.Marshal(r.Labels)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal labels: %w", err)
	}

	var createdAt, updatedAt time.Time
	var resourceVersion int64

	err = s.db.QueryRowContext(ctx, `
		INSERT INTO records (type, tradespace, name, labels, data)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING resource_version, created_at, updated_at
	`, r.Type, r.Tradespace, r.Name, labels, r.Data).Scan(&resourceVersion, &createdAt, &updatedAt)

	if err != nil {
		if isUniqueViolation(err) {
			return nil, storage.ErrAlreadyExists
		}
		return nil, fmt.Errorf("failed to create record: %w", err)
	}

	return &storage.Row{
		Type:            r.Type,
		Tradespace:      r.Tradespace,
		Name:            r.Name,
		Labels:          r.Labels,
		Data:            r.Data,
		ResourceVersion: resourceVersion,
		CreatedAt:       createdAt,
		UpdatedAt:       updatedAt,
	}, nil
}

// Get retrieves a row by key.
func (s *RowStorage) Get(ctx context.Context, key storage.Key) (*storage.Row, error) {
	var labelsJSON []byte
	row := &storage.Row{}

	err := s.db.QueryRowContext(ctx, `
		SELECT type, tradespace, name, labels, data, resource_version, created_at, updated_at
		FROM records
		WHERE type = $1 AND tradespace = $2 AND name = $3
	`, key.Type, key.Tradespace, key.Name).Scan(
		&row.Type, &row.Tradespace, &row.Name, &labelsJSON, &row.Data,
		&row.ResourceVersion, &row.CreatedAt, &row.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, storage.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get record: %w", err)
	}

	if err := json.Unmarshal(labelsJSON, &row.Labels); err != nil {
		return nil, fmt.Errorf("failed to unmarshal labels: %w", err)
	}

	return row, nil
}

// Update updates an existing row with optimistic locking.
func (s *RowStorage) Update(ctx context.Context, r *storage.Row) (*storage.Row, error) {
	labels, err := json.Marshal(r.Labels)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal labels: %w", err)
	}

	var updatedAt time.Time
	var newVersion int64

	// Optimistic locking with resource_version
	err = s.db.QueryRowContext(ctx, `
		UPDATE records
		SET labels = $1, data = $2, resource_version = resource_version + 1, updated_at = NOW()
		WHERE type = $3 AND tradespace = $4 AND name = $5 AND resource_version = $6
		RETURNING resource_version, updated_at
	`, labels, r.Data, r.Type, r.Tradespace, r.Name, r.ResourceVersion).Scan(&newVersion, &updatedAt)

	if err != nil {
		if err == sql.ErrNoRows {
			// Check if record exists at all
			var exists bool
			s.db.QueryRowContext(ctx, `
				SELECT EXISTS(SELECT 1 FROM records WHERE type = $1 AND tradespace = $2 AND name = $3)
			`, r.Type, r.Tradespace, r.Name).Scan(&exists)
			if !exists {
				return nil, storage.ErrNotFound
			}
			return nil, fmt.Errorf("conflict: resource version mismatch")
		}
		return nil, fmt.Errorf("failed to update record: %w", err)
	}

	return &storage.Row{
		Type:            r.Type,
		Tradespace:      r.Tradespace,
		Name:            r.Name,
		Labels:          r.Labels,
		Data:            r.Data,
		ResourceVersion: newVersion,
		CreatedAt:       r.CreatedAt,
		UpdatedAt:       updatedAt,
	}, nil
}

// Delete removes a row.
func (s *RowStorage) Delete(ctx context.Context, key storage.Key) error {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM records
		WHERE type = $1 AND tradespace = $2 AND name = $3
	`, key.Type, key.Tradespace, key.Name)
	if err != nil {
		return fmt.Errorf("failed to delete record: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rows == 0 {
		return storage.ErrNotFound
	}

	return nil
}

// List returns rows matching the query.
func (s *RowStorage) List(ctx context.Context, q storage.Query) ([]*storage.Row, error) {
	var args []any
	var conditions []string
	argNum := 1

	// Type is required
	conditions = append(conditions, fmt.Sprintf("type = $%d", argNum))
	args = append(args, q.Type)
	argNum++

	// Optional tradespace filter
	if q.Tradespace != "" {
		conditions = append(conditions, fmt.Sprintf("tradespace = $%d", argNum))
		args = append(args, q.Tradespace)
		argNum++
	}

	// Optional name filter
	if q.Name != "" {
		conditions = append(conditions, fmt.Sprintf("name = $%d", argNum))
		args = append(args, q.Name)
		argNum++
	}

	// Optional label filters using JSONB containment
	if len(q.Labels) > 0 {
		labelsJSON, _ := json.Marshal(q.Labels)
		conditions = append(conditions, fmt.Sprintf("labels @> $%d", argNum))
		args = append(args, labelsJSON)
		argNum++
	}

	query := fmt.Sprintf(`
		SELECT type, tradespace, name, labels, data, resource_version, created_at, updated_at
		FROM records
		WHERE %s
		ORDER BY created_at ASC
	`, strings.Join(conditions, " AND "))

	if q.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", q.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list records: %w", err)
	}
	defer rows.Close()

	var results []*storage.Row
	for rows.Next() {
		var labelsJSON []byte
		row := &storage.Row{}

		err := rows.Scan(
			&row.Type, &row.Tradespace, &row.Name, &labelsJSON, &row.Data,
			&row.ResourceVersion, &row.CreatedAt, &row.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		if err := json.Unmarshal(labelsJSON, &row.Labels); err != nil {
			return nil, fmt.Errorf("failed to unmarshal labels: %w", err)
		}

		results = append(results, row)
	}

	return results, rows.Err()
}

// Close closes the database connection.
func (s *RowStorage) Close() error {
	return s.db.Close()
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "duplicate key")
}
