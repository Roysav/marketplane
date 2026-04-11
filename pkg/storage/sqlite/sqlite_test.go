package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/roysav/marketplane/pkg/storage"
)

func newTestStorage(t *testing.T) *Storage {
	t.Helper()
	s, err := New(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCreate(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	r := &storage.Row{
		Type:       "core/v1/Tradespace",
		Tradespace: "default",
		Name:       "production",
		Labels:     map[string]string{"env": "prod"},
		Data:       `{"description":"Production environment"}`,
	}

	created, err := s.Create(ctx, r)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if created.ResourceVersion != 1 {
		t.Errorf("ResourceVersion = %d, want 1", created.ResourceVersion)
	}
	if created.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
}

func TestCreate_Duplicate(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	r := &storage.Row{
		Type:       "core/v1/Tradespace",
		Tradespace: "default",
		Name:       "test",
	}

	if _, err := s.Create(ctx, r); err != nil {
		t.Fatalf("first Create failed: %v", err)
	}

	_, err := s.Create(ctx, r)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got: %v", err)
	}
}

func TestGet(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	r := &storage.Row{
		Type:       "test/v1/Foo",
		Tradespace: "ns1",
		Name:       "bar",
		Data:       `{"key":"value"}`,
	}
	s.Create(ctx, r)

	got, err := s.Get(ctx, storage.Key{
		Type:       "test/v1/Foo",
		Tradespace: "ns1",
		Name:       "bar",
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.Data != `{"key":"value"}` {
		t.Errorf("Data mismatch: %s", got.Data)
	}
}

func TestGet_NotFound(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	_, err := s.Get(ctx, storage.Key{
		Type:       "x/v1/Y",
		Tradespace: "default",
		Name:       "nope",
	})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestUpdate(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	r := &storage.Row{
		Type:       "test/v1/Item",
		Tradespace: "default",
		Name:       "item1",
		Data:       `{"count":1}`,
	}
	created, err := s.Create(ctx, r)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	created.Data = `{"count":2}`
	updated, err := s.Update(ctx, created)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if updated.ResourceVersion != 2 {
		t.Errorf("ResourceVersion = %d, want 2", updated.ResourceVersion)
	}
	if updated.Data != `{"count":2}` {
		t.Errorf("Data not updated: %s", updated.Data)
	}
}

func TestUpdate_NotFound(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	r := &storage.Row{
		Type:       "test/v1/Item",
		Tradespace: "default",
		Name:       "nonexistent",
	}

	_, err := s.Update(ctx, r)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestDelete(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	r := &storage.Row{
		Type:       "test/v1/Item",
		Tradespace: "default",
		Name:       "todelete",
	}
	s.Create(ctx, r)

	err := s.Delete(ctx, r.Key())
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = s.Get(ctx, r.Key())
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got: %v", err)
	}
}

func TestDelete_NotFound(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	err := s.Delete(ctx, storage.Key{
		Type:       "x/v1/Y",
		Tradespace: "default",
		Name:       "nope",
	})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestList(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	for _, name := range []string{"a", "b", "c"} {
		s.Create(ctx, &storage.Row{
			Type:       "test/v1/Item",
			Tradespace: "default",
			Name:       name,
		})
	}

	records, err := s.List(ctx, storage.Query{Type: "test/v1/Item"})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(records) != 3 {
		t.Errorf("List returned %d records, want 3", len(records))
	}

	records, err = s.List(ctx, storage.Query{Type: "test/v1/Item", Limit: 2})
	if err != nil {
		t.Fatalf("List with limit failed: %v", err)
	}
	if len(records) != 2 {
		t.Errorf("List with limit returned %d records, want 2", len(records))
	}
}

func TestList_ByTradespace(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	s.Create(ctx, &storage.Row{Type: "test/v1/Item", Tradespace: "ns1", Name: "a"})
	s.Create(ctx, &storage.Row{Type: "test/v1/Item", Tradespace: "ns2", Name: "b"})

	records, err := s.List(ctx, storage.Query{Type: "test/v1/Item", Tradespace: "ns1"})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("List returned %d records, want 1", len(records))
	}
	if records[0].Name != "a" {
		t.Errorf("Expected record 'a', got %s", records[0].Name)
	}
}

func TestList_ByLabels(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	s.Create(ctx, &storage.Row{
		Type:       "test/v1/Item",
		Tradespace: "default",
		Name:       "a",
		Labels:     map[string]string{"env": "prod"},
	})
	s.Create(ctx, &storage.Row{
		Type:       "test/v1/Item",
		Tradespace: "default",
		Name:       "b",
		Labels:     map[string]string{"env": "dev"},
	})

	records, err := s.List(ctx, storage.Query{
		Type:   "test/v1/Item",
		Labels: map[string]string{"env": "prod"},
	})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("List returned %d records, want 1", len(records))
	}
	if records[0].Name != "a" {
		t.Errorf("Expected record 'a', got %s", records[0].Name)
	}
}
