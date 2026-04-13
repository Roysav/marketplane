package postgres

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/roysav/marketplane/pkg/storage"
)

const defaultTestDSN = "postgres://marketplane:marketplane@localhost:5432/marketplane?sslmode=disable"

func newTestStorage(t *testing.T) *Storage {
	t.Helper()
	ctx := context.Background()

	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		dsn = defaultTestDSN
	}

	pool, err := pgxpool.New(ctx, dsn)
	s := New(pool)
	if err != nil {
		t.Skipf("postgres not available: %v", err)
	}

	_, err = s.pool.Exec(ctx, `TRUNCATE TABLE records`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}

	t.Cleanup(func() { s.Close() })
	return s
}

func TestStorage_CreateAndGet(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	row := &storage.Row{
		Key:    "core/v1/Tradespace/default/test",
		Data:   []byte(`{"description": "hello"}`),
		Labels: map[string]string{"env": "test"},
	}

	created, err := s.Create(ctx, row)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if string(created.Data) != string(row.Data) {
		t.Errorf("data mismatch: got %s", created.Data)
	}

	got, err := s.Get(ctx, row.Key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Key != row.Key {
		t.Errorf("key: want %s, got %s", row.Key, got.Key)
	}
	if string(got.Data) != string(row.Data) {
		t.Errorf("data: want %s, got %s", row.Data, got.Data)
	}
	if got.Labels["env"] != "test" {
		t.Errorf("labels: want env=test, got %v", got.Labels)
	}
}

func TestStorage_CreateDuplicate(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	row := &storage.Row{Key: "dup/key", Data: []byte(`{}`)}
	if _, err := s.Create(ctx, row); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	_, err := s.Create(ctx, row)
	if err == nil {
		t.Fatal("expected error on duplicate create")
	}
	if !isError(err, storage.ErrAlreadyExists) {
		t.Errorf("want ErrAlreadyExists, got: %v", err)
	}
}

func TestStorage_GetNotFound(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	_, err := s.Get(ctx, "nonexistent/key")
	if !isError(err, storage.ErrNotFound) {
		t.Errorf("want ErrNotFound, got: %v", err)
	}
}

func TestStorage_Update(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	row := &storage.Row{Key: "update/key", Data: []byte(`{"v": 1}`)}
	if _, err := s.Create(ctx, row); err != nil {
		t.Fatalf("Create: %v", err)
	}

	row.Data = []byte(`{"v": 2}`)
	updated, err := s.Update(ctx, row, []byte(""))
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if string(updated.Data) != `{"v": 2}` {
		t.Errorf("updated data: got %s", updated.Data)
	}

	got, _ := s.Get(ctx, row.Key)
	if string(got.Data) != `{"v": 2}` {
		t.Errorf("persisted data: got %s", got.Data)
	}
}

func TestStorage_UpdateNotFound(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	_, err := s.Update(ctx, &storage.Row{Key: "missing/key", Data: []byte(`{}`)}, []byte(""))
	if !isError(err, storage.ErrNotFound) {
		t.Errorf("want ErrNotFound, got: %v", err)
	}
}

func TestStorage_Delete(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	row := &storage.Row{Key: "delete/key", Data: []byte(`{}`)}
	if _, err := s.Create(ctx, row); err != nil {
		t.Fatalf("Create: %v", err)
	}

	if err := s.Delete(ctx, row.Key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := s.Get(ctx, row.Key)
	if !isError(err, storage.ErrNotFound) {
		t.Errorf("want ErrNotFound after delete, got: %v", err)
	}
}

func TestStorage_DeleteNotFound(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	err := s.Delete(ctx, "missing/key")
	if !isError(err, storage.ErrNotFound) {
		t.Errorf("want ErrNotFound, got: %v", err)
	}
}

func TestStorage_ListByPrefix(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	rows := []*storage.Row{
		{Key: "orders/v1/a", Data: []byte(`{}`)},
		{Key: "orders/v1/b", Data: []byte(`{}`)},
		{Key: "other/v1/c", Data: []byte(`{}`)},
	}
	for _, r := range rows {
		if _, err := s.Create(ctx, r); err != nil {
			t.Fatalf("Create %s: %v", r.Key, err)
		}
	}

	got, err := s.List(ctx, storage.Query{Prefix: "orders/"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("want 2 rows, got %d", len(got))
	}
}

func TestStorage_ListByLabels(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	rows := []*storage.Row{
		{Key: "k1", Data: []byte(`{}`), Labels: map[string]string{"env": "prod", "app": "api"}},
		{Key: "k2", Data: []byte(`{}`), Labels: map[string]string{"env": "prod", "app": "worker"}},
		{Key: "k3", Data: []byte(`{}`), Labels: map[string]string{"env": "staging"}},
	}
	for _, r := range rows {
		if _, err := s.Create(ctx, r); err != nil {
			t.Fatalf("Create %s: %v", r.Key, err)
		}
	}

	got, err := s.List(ctx, storage.Query{Labels: map[string]string{"env": "prod"}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("want 2 rows, got %d", len(got))
	}

	got, err = s.List(ctx, storage.Query{Labels: map[string]string{"env": "prod", "app": "api"}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("want 1 row, got %d", len(got))
	}
}

func TestStorage_ListLimit(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	for i := range 5 {
		r := &storage.Row{Key: string(rune('a'+i)) + "/key", Data: []byte(`{}`)}
		if _, err := s.Create(ctx, r); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	got, err := s.List(ctx, storage.Query{Limit: 3})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("want 3 rows, got %d", len(got))
	}
}

// isError checks if err wraps target.
func isError(err, target error) bool {
	if err == nil {
		return false
	}
	// errors.Is handles wrapped errors
	var check func(error) bool
	check = func(e error) bool {
		if e == target {
			return true
		}
		if u, ok := e.(interface{ Unwrap() error }); ok {
			return check(u.Unwrap())
		}
		return false
	}
	return check(err)
}
