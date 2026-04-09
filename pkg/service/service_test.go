package service

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/roysav/marketplane/pkg/entity"
	"github.com/roysav/marketplane/pkg/storage/sqlite"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	ctx := context.Background()

	records, err := sqlite.New(ctx, ":memory:")
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	t.Cleanup(func() { records.Close() })

	// Use a discarding logger for tests
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	return New(Config{
		Records: records,
		Events:  nil, // no events for basic tests
		Logger:  log,
	})
}

func TestService_CreateAndGet(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	e := &entity.Entity{
		TypeMeta: entity.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
		ObjectMeta: entity.ObjectMeta{
			Tradespace: "default",
			Name:       "test-tradespace",
			Labels:     map[string]string{"env": "test"},
		},
		Spec: map[string]any{"description": "A test tradespace"},
	}

	// Create
	created, err := svc.Create(ctx, e)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if created.ObjectMeta.ResourceVersion != 1 {
		t.Errorf("expected ResourceVersion 1, got %d", created.ObjectMeta.ResourceVersion)
	}

	// Get
	got, err := svc.Get(ctx, "core/v1/Tradespace", "default", "test-tradespace")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if got.ObjectMeta.Name != "test-tradespace" {
		t.Errorf("expected name test-tradespace, got %s", got.ObjectMeta.Name)
	}
	if got.Spec["description"] != "A test tradespace" {
		t.Errorf("unexpected spec: %v", got.Spec)
	}
}

func TestService_CreateDuplicate(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	e := &entity.Entity{
		TypeMeta: entity.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
		ObjectMeta: entity.ObjectMeta{
			Tradespace: "default",
			Name:       "dup-test",
		},
		Spec: map[string]any{},
	}

	_, err := svc.Create(ctx, e)
	if err != nil {
		t.Fatalf("first Create failed: %v", err)
	}

	_, err = svc.Create(ctx, e)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got: %v", err)
	}
}

func TestService_Update(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	e := &entity.Entity{
		TypeMeta: entity.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
		ObjectMeta: entity.ObjectMeta{
			Tradespace: "default",
			Name:       "update-test",
		},
		Spec: map[string]any{"description": "original"},
	}

	created, err := svc.Create(ctx, e)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Update
	created.Spec["description"] = "updated"
	updated, err := svc.Update(ctx, created)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if updated.ObjectMeta.ResourceVersion != 2 {
		t.Errorf("expected ResourceVersion 2, got %d", updated.ObjectMeta.ResourceVersion)
	}

	// Verify
	got, _ := svc.Get(ctx, "core/v1/Tradespace", "default", "update-test")
	if got.Spec["description"] != "updated" {
		t.Errorf("expected updated description, got: %v", got.Spec)
	}
}

func TestService_Delete(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	e := &entity.Entity{
		TypeMeta: entity.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
		ObjectMeta: entity.ObjectMeta{
			Tradespace: "default",
			Name:       "delete-test",
		},
		Spec: map[string]any{},
	}

	_, err := svc.Create(ctx, e)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	err = svc.Delete(ctx, "core/v1/Tradespace", "default", "delete-test")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = svc.Get(ctx, "core/v1/Tradespace", "default", "delete-test")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got: %v", err)
	}
}

func TestService_List(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	// Create multiple entities
	for _, name := range []string{"ts-1", "ts-2", "ts-3"} {
		e := &entity.Entity{
			TypeMeta: entity.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
			ObjectMeta: entity.ObjectMeta{
				Tradespace: "default",
				Name:       name,
			},
			Spec: map[string]any{},
		}
		_, err := svc.Create(ctx, e)
		if err != nil {
			t.Fatalf("Create %s failed: %v", name, err)
		}
	}

	// List all
	entities, err := svc.List(ctx, "core/v1/Tradespace", "", nil)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(entities) != 3 {
		t.Errorf("expected 3 entities, got %d", len(entities))
	}
}

func TestService_ListByTradespace(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	// Create entities in different tradespaces
	for _, ts := range []string{"prod", "staging", "prod"} {
		e := &entity.Entity{
			TypeMeta: entity.TypeMeta{Group: "core", Version: "v1", Kind: "Quota"},
			ObjectMeta: entity.ObjectMeta{
				Tradespace: ts,
				Name:       "quota-" + ts + "-" + randomSuffix(),
			},
			Spec: map[string]any{"balances": map[string]any{"USD": "100"}},
		}
		_, err := svc.Create(ctx, e)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
	}

	// List only prod
	entities, err := svc.List(ctx, "core/v1/Quota", "prod", nil)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(entities) != 2 {
		t.Errorf("expected 2 entities in prod, got %d", len(entities))
	}
}

func TestService_ValidationError(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	// Missing required field for Quota
	e := &entity.Entity{
		TypeMeta: entity.TypeMeta{Group: "core", Version: "v1", Kind: "Quota"},
		ObjectMeta: entity.ObjectMeta{
			Tradespace: "default",
			Name:       "invalid-quota",
		},
		Spec: map[string]any{}, // missing "balances"
	}

	_, err := svc.Create(ctx, e)
	if !errors.Is(err, ErrValidation) {
		t.Errorf("expected ErrValidation, got: %v", err)
	}
}

func TestService_GetNotFound(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	_, err := svc.Get(ctx, "core/v1/Tradespace", "default", "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

var counter int

func randomSuffix() string {
	counter++
	return string(rune('a' + counter%26))
}
