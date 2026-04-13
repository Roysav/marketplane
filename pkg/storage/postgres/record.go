package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/roysav/marketplane/pkg/storage"
)

// Storage implements storage.RowStorage using PostgreSQL.
type Storage struct {
	pool *pgxpool.Pool
}

// New creates a Storage from an existing connection pool.
func New(pool *pgxpool.Pool) *Storage {
	return &Storage{pool: pool}
}

func (s *Storage) Create(ctx context.Context, r *storage.Row) (*storage.Row, error) {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO records (key, data, labels) VALUES ($1, $2, $3)`,
		r.Key, r.Data, labelsToArray(r.Labels),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, fmt.Errorf("%w: %s", storage.ErrAlreadyExists, r.Key)
		}
		return nil, fmt.Errorf("postgres: create: %w", err)
	}
	return r, nil
}

func (s *Storage) Get(ctx context.Context, key string) (*storage.Row, error) {
	row := &storage.Row{Key: key}
	var labels []string
	err := s.pool.QueryRow(ctx,
		`SELECT data, labels FROM records WHERE key = $1`,
		key,
	).Scan(&row.Data, &labels)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", storage.ErrNotFound, key)
		}
		return nil, fmt.Errorf("postgres: get: %w", err)
	}
	row.Labels = arrayToLabels(labels)
	return row, nil
}

func (s *Storage) Update(ctx context.Context, r *storage.Row, lastApplied []byte) (*storage.Row, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE records SET data = $2, labels = $3 WHERE key = $1`,
		r.Key, r.Data, labelsToArray(r.Labels),
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, fmt.Errorf("%w: %s", storage.ErrNotFound, r.Key)
	}
	return r, nil
}

func (s *Storage) Delete(ctx context.Context, key string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM records WHERE key = $1`, key)
	if err != nil {
		return fmt.Errorf("postgres: delete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", storage.ErrNotFound, key)
	}
	return nil
}

func (s *Storage) List(ctx context.Context, q storage.Query) ([]*storage.Row, error) {
	query := `SELECT key, data, labels FROM records`
	var args []any
	var conds []string

	if q.Prefix != "" {
		args = append(args, q.Prefix+"%")
		conds = append(conds, fmt.Sprintf("key LIKE $%d", len(args)))
	}
	if len(q.Labels) > 0 {
		args = append(args, labelsToArray(q.Labels))
		conds = append(conds, fmt.Sprintf("labels @> $%d", len(args)))
	}
	if len(conds) > 0 {
		query += " WHERE " + strings.Join(conds, " AND ")
	}
	if q.Limit > 0 {
		args = append(args, q.Limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: list: %w", err)
	}
	defer rows.Close()

	var result []*storage.Row
	for rows.Next() {
		row := &storage.Row{}
		var labels []string
		if err := rows.Scan(&row.Key, &row.Data, &labels); err != nil {
			return nil, fmt.Errorf("postgres: list scan: %w", err)
		}
		row.Labels = arrayToLabels(labels)
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Storage) Close() error {
	s.pool.Close()
	return nil
}

// labelsToArray serializes a labels map as ["key=value", ...] for PostgreSQL array storage.
func labelsToArray(labels map[string]string) []string {
	arr := make([]string, 0, len(labels))
	for k, v := range labels {
		arr = append(arr, k+"="+v)
	}
	return arr
}

// arrayToLabels deserializes a ["key=value", ...] PostgreSQL array back into a map.
func arrayToLabels(arr []string) map[string]string {
	labels := make(map[string]string, len(arr))
	for _, s := range arr {
		k, v, _ := strings.Cut(s, "=")
		labels[k] = v
	}
	return labels
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
