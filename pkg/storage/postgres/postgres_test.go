package postgres

import (
	"context"
	"testing"

	"github.com/roysav/marketplane/pkg/storage"
)

const testDSN = "postgres://marketplane:marketplane@localhost:5432/marketplane?sslmode=disable"

func newTestRowStorage(t *testing.T) *RowStorage {
	t.Helper()
	ctx := context.Background()

	s, err := New(ctx, testDSN)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}

	// Clean up tables for test isolation
	s.db.ExecContext(ctx, "TRUNCATE records CASCADE")

	t.Cleanup(func() { s.Close() })
	return s
}

func TestRowStorage_CreateAndGet(t *testing.T) {
	s := newTestRowStorage(t)
	ctx := context.Background()

	row := &storage.Row{
		Type:       "core/v1/Tradespace",
		Tradespace: "default",
		Name:       "test-ns",
		Labels:     map[string]string{"env": "test"},
		Data:       `{"spec":{"description":"test"}}`,
	}

	created, err := s.Create(ctx, row)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if created.ResourceVersion != 1 {
		t.Errorf("ResourceVersion = %d, want 1", created.ResourceVersion)
	}

	got, err := s.Get(ctx, storage.Key{
		Type:       "core/v1/Tradespace",
		Tradespace: "default",
		Name:       "test-ns",
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Name != "test-ns" {
		t.Errorf("Name = %s, want test-ns", got.Name)
	}
	if got.Labels["env"] != "test" {
		t.Errorf("Labels[env] = %s, want test", got.Labels["env"])
	}
}

func TestRowStorage_CreateDuplicate(t *testing.T) {
	s := newTestRowStorage(t)
	ctx := context.Background()

	row := &storage.Row{
		Type:       "core/v1/Tradespace",
		Tradespace: "default",
		Name:       "dup-test",
		Data:       "{}",
	}

	_, err := s.Create(ctx, row)
	if err != nil {
		t.Fatalf("First Create failed: %v", err)
	}

	_, err = s.Create(ctx, row)
	if err != storage.ErrAlreadyExists {
		t.Errorf("Second Create error = %v, want ErrAlreadyExists", err)
	}
}

func TestRowStorage_Update(t *testing.T) {
	s := newTestRowStorage(t)
	ctx := context.Background()

	row := &storage.Row{
		Type:       "core/v1/Tradespace",
		Tradespace: "default",
		Name:       "update-test",
		Labels:     map[string]string{"v": "1"},
		Data:       `{"version":1}`,
	}

	created, err := s.Create(ctx, row)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	created.Labels["v"] = "2"
	created.Data = `{"version":2}`

	updated, err := s.Update(ctx, created)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if updated.ResourceVersion != 2 {
		t.Errorf("ResourceVersion = %d, want 2", updated.ResourceVersion)
	}

	got, _ := s.Get(ctx, created.Key())
	if got.Labels["v"] != "2" {
		t.Errorf("Labels[v] = %s, want 2", got.Labels["v"])
	}
}

func TestRowStorage_UpdateOptimisticLock(t *testing.T) {
	s := newTestRowStorage(t)
	ctx := context.Background()

	row := &storage.Row{
		Type:       "core/v1/Tradespace",
		Tradespace: "default",
		Name:       "lock-test",
		Data:       "{}",
	}

	created, _ := s.Create(ctx, row)

	// First update should succeed
	created.Data = `{"v":1}`
	updated, err := s.Update(ctx, created)
	if err != nil {
		t.Fatalf("First update failed: %v", err)
	}

	// Second update with old version should fail
	created.Data = `{"v":2}`
	_, err = s.Update(ctx, created)
	if err == nil {
		t.Error("Update with stale version should fail")
	}

	// Update with correct version should succeed
	updated.Data = `{"v":3}`
	_, err = s.Update(ctx, updated)
	if err != nil {
		t.Errorf("Update with correct version failed: %v", err)
	}
}

func TestRowStorage_Delete(t *testing.T) {
	s := newTestRowStorage(t)
	ctx := context.Background()

	row := &storage.Row{
		Type:       "core/v1/Tradespace",
		Tradespace: "default",
		Name:       "delete-test",
		Data:       "{}",
	}

	s.Create(ctx, row)

	err := s.Delete(ctx, row.Key())
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = s.Get(ctx, row.Key())
	if err != storage.ErrNotFound {
		t.Errorf("Get after delete error = %v, want ErrNotFound", err)
	}
}

func TestRowStorage_List(t *testing.T) {
	s := newTestRowStorage(t)
	ctx := context.Background()

	// Create test records
	for i := 0; i < 3; i++ {
		s.Create(ctx, &storage.Row{
			Type:       "core/v1/Tradespace",
			Tradespace: "default",
			Name:       string(rune('a' + i)),
			Data:       "{}",
		})
	}

	rows, err := s.List(ctx, storage.Query{Type: "core/v1/Tradespace"})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("List returned %d rows, want 3", len(rows))
	}
}

func TestRowStorage_ListByLabels(t *testing.T) {
	s := newTestRowStorage(t)
	ctx := context.Background()

	s.Create(ctx, &storage.Row{
		Type:       "test/v1/Record",
		Tradespace: "default",
		Name:       "labeled",
		Labels:     map[string]string{"app": "test", "env": "prod"},
		Data:       "{}",
	})

	s.Create(ctx, &storage.Row{
		Type:       "test/v1/Record",
		Tradespace: "default",
		Name:       "unlabeled",
		Data:       "{}",
	})

	rows, err := s.List(ctx, storage.Query{
		Type:   "test/v1/Record",
		Labels: map[string]string{"app": "test"},
	})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(rows) != 1 {
		t.Errorf("List returned %d rows, want 1", len(rows))
	}
	if rows[0].Name != "labeled" {
		t.Errorf("Name = %s, want labeled", rows[0].Name)
	}
}
