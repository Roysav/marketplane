package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/service"
	"github.com/roysav/marketplane/tests"
)

func newTestService(t *testing.T) *service.Service {
	t.Helper()
	ctx := context.Background()
	return tests.SVC(ctx, t)
}

func TestService_CreateAndGet(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	r := &record.Record{
		Type: "core/v1/Tradespace",
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "test-tradespace",
			Labels:     map[string]string{"env": "test"},
		},
		Spec: map[string]any{"description": "A test tradespace", "name": "test-tradespace"},
	}

	// Create
	created, err := svc.Create(ctx, r)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if created.ObjectMeta.Name != "test-tradespace" {
		t.Errorf("Expected object name %s, actual %s", "test-tradespace", created.ObjectMeta.Name)
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

	r := &record.Record{
		Type: "core/v1/Tradespace",
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "dup-test",
		},
		Spec: map[string]any{},
	}

	_, err := svc.Create(ctx, r)
	if err != nil {
		t.Fatalf("first Create failed: %v", err)
	}

	_, err = svc.Create(ctx, r)
	if !errors.Is(err, service.ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got: %v", err)
	}
}

func TestService_Update(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	r := &record.Record{
		Type: "core/v1/Tradespace",
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "update-test",
		},
		Spec: map[string]any{"description": "original"},
	}

	created, err := svc.Create(ctx, r)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Update
	created.Spec["description"] = "updated"
	updated, err := svc.Update(ctx, created)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	assert.Equal(t, "updated", updated.Spec["description"])

	// Verify
	got, _ := svc.Get(ctx, "core/v1/Tradespace", "default", "update-test")
	if got.Spec["description"] != "updated" {
		t.Errorf("expected updated description, got: %v", got.Spec)
	}
}

func TestService_Delete(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	r := &record.Record{
		Type: "core/v1/Tradespace",
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "delete-test",
		},
		Spec: map[string]any{},
	}

	_, err := svc.Create(ctx, r)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	err = svc.Delete(ctx, "core/v1/Tradespace", "default", "delete-test")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = svc.Get(ctx, "core/v1/Tradespace", "default", "delete-test")
	if !errors.Is(err, service.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got: %v", err)
	}
}

func TestService_List(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	// Create multiple records
	for _, name := range []string{"ts-1", "ts-2", "ts-3"} {
		r := &record.Record{
			Type: "core/v1/Tradespace",
			ObjectMeta: record.ObjectMeta{
				Tradespace: "default",
				Name:       name,
			},
			Spec: map[string]any{},
		}
		_, err := svc.Create(ctx, r)
		if err != nil {
			t.Fatalf("Create %s failed: %v", name, err)
		}
	}

	// List all
	records, err := svc.List(ctx, "core/v1/Tradespace", "", nil)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(records) != 3 {
		t.Errorf("expected 3 records, got %d", len(records))
	}
}

func TestService_ListByTradespace(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	// Create records in different tradespaces
	for _, ts := range []string{"prod", "staging", "prod"} {
		r := &record.Record{
			Type: "core/v1/Tradespace",
			ObjectMeta: record.ObjectMeta{
				Tradespace: ts,
				Name:       "ts-" + ts + "-" + randomSuffix(),
			},
			Spec: map[string]any{},
		}
		_, err := svc.Create(ctx, r)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
	}

	// List only prod
	records, err := svc.List(ctx, "core/v1/Tradespace", "prod", nil)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(records) != 2 {
		t.Errorf("expected 2 records in prod, got %d", len(records))
	}
}

func TestService_ValidationError(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	// Missing required field "balances" for Quota
	r := &record.Record{
		Type: "core/v1/Quota",
		ObjectMeta: record.ObjectMeta{
			Tradespace: "myns",
			Name:       "invalid-quota",
		},
		Spec: map[string]any{}, // missing "balances"
	}

	_, err := svc.Create(ctx, r)
	if !errors.Is(err, service.ErrValidation) {
		t.Errorf("expected ErrValidation, got: %v", err)
	}
}

func TestService_GetNotFound(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	_, err := svc.Get(ctx, "core/v1/Tradespace", "default", "nonexistent")
	if !errors.Is(err, service.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

var counter int

func randomSuffix() string {
	counter++
	return string(rune('a' + counter%26))
}
