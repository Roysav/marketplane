package service

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/storage/sqlite"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	ctx := context.Background()

	rows, err := sqlite.New(ctx, ":memory:")
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	t.Cleanup(func() { rows.Close() })

	// Use a discarding logger for tests
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	return New(Config{
		Rows:   rows,
		Events: nil, // no events for basic tests
		Logger: logger,
	})
}

func TestService_CreateAndGet(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "test-tradespace",
			Labels:     map[string]string{"env": "test"},
		},
		Spec: map[string]any{"description": "A test tradespace"},
	}

	// Create
	created, err := svc.Create(ctx, r)
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

	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
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
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got: %v", err)
	}
}

func TestService_Update(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
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

	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
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
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got: %v", err)
	}
}

func TestService_List(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	// Create multiple records
	for _, name := range []string{"ts-1", "ts-2", "ts-3"} {
		r := &record.Record{
			TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
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
			TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Quota"},
			ObjectMeta: record.ObjectMeta{
				Tradespace: ts,
				Name:       "quota-" + ts + "-" + randomSuffix(),
			},
			Spec: map[string]any{"balances": map[string]any{"USD": "100"}},
		}
		_, err := svc.Create(ctx, r)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
	}

	// List only prod
	records, err := svc.List(ctx, "core/v1/Quota", "prod", nil)
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

	// Missing required field for Quota
	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Quota"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "myns",
			Name:       "invalid-quota",
		},
		Spec: map[string]any{}, // missing "balances"
	}

	_, err := svc.Create(ctx, r)
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

// ---------------------------------------------------------------------------
// Finalizer tests
// ---------------------------------------------------------------------------

func TestService_CreateWithFinalizers(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "ts-with-finalizers",
			Finalizers: []string{"cleanup.example.com", "billing.example.com"},
		},
		Spec: map[string]any{},
	}

	created, err := svc.Create(ctx, r)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if len(created.ObjectMeta.Finalizers) != 2 {
		t.Errorf("expected 2 finalizers, got %d: %v", len(created.ObjectMeta.Finalizers), created.ObjectMeta.Finalizers)
	}

	got, err := svc.Get(ctx, "core/v1/Tradespace", "default", "ts-with-finalizers")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(got.ObjectMeta.Finalizers) != 2 {
		t.Errorf("expected 2 finalizers after round-trip, got %d", len(got.ObjectMeta.Finalizers))
	}
	if got.ObjectMeta.DeletionTimestamp != nil {
		t.Error("DeletionTimestamp should be nil for a fresh record")
	}
}

func TestService_DeleteNoFinalizers_HardDelete(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "ts-no-finalizers",
		},
		Spec: map[string]any{},
	}
	if _, err := svc.Create(ctx, r); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if err := svc.Delete(ctx, "core/v1/Tradespace", "default", "ts-no-finalizers"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Record must be gone (hard delete).
	_, err := svc.Get(ctx, "core/v1/Tradespace", "default", "ts-no-finalizers")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound after hard delete, got: %v", err)
	}
}

func TestService_DeleteWithFinalizers_SoftDelete(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "ts-soft-delete",
			Finalizers: []string{"cleanup.example.com"},
		},
		Spec: map[string]any{},
	}
	if _, err := svc.Create(ctx, r); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if err := svc.Delete(ctx, "core/v1/Tradespace", "default", "ts-soft-delete"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Record must still exist (soft delete only).
	got, err := svc.Get(ctx, "core/v1/Tradespace", "default", "ts-soft-delete")
	if err != nil {
		t.Fatalf("expected record to still exist after soft delete, got: %v", err)
	}
	if got.ObjectMeta.DeletionTimestamp == nil {
		t.Error("expected DeletionTimestamp to be set after soft delete")
	}
	if !got.ObjectMeta.IsTerminating() {
		t.Error("expected IsTerminating() == true after soft delete")
	}
	if len(got.ObjectMeta.Finalizers) != 1 {
		t.Errorf("expected finalizers to be preserved, got: %v", got.ObjectMeta.Finalizers)
	}
}

func TestService_DeleteIdempotent_AlreadyTerminating(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "ts-idempotent-delete",
			Finalizers: []string{"a.example.com"},
		},
		Spec: map[string]any{},
	}
	if _, err := svc.Create(ctx, r); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// First delete — soft delete.
	if err := svc.Delete(ctx, "core/v1/Tradespace", "default", "ts-idempotent-delete"); err != nil {
		t.Fatalf("first Delete failed: %v", err)
	}

	// Second delete on the already-terminating record must be a no-op.
	if err := svc.Delete(ctx, "core/v1/Tradespace", "default", "ts-idempotent-delete"); err != nil {
		t.Errorf("second Delete on terminating record should be idempotent, got: %v", err)
	}
}

func TestService_DeleteNotFound(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	err := svc.Delete(ctx, "core/v1/Tradespace", "default", "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestService_UpdateCannotAddFinalizersToTerminating(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "ts-no-add-finalizer",
			Finalizers: []string{"existing.example.com"},
		},
		Spec: map[string]any{},
	}
	if _, err := svc.Create(ctx, r); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Soft-delete the record.
	if err := svc.Delete(ctx, "core/v1/Tradespace", "default", "ts-no-add-finalizer"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Fetch the now-terminating record.
	terminating, err := svc.Get(ctx, "core/v1/Tradespace", "default", "ts-no-add-finalizer")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Attempt to add a new finalizer — must be rejected.
	terminating.ObjectMeta.Finalizers = append(terminating.ObjectMeta.Finalizers, "new.example.com")
	_, err = svc.Update(ctx, terminating)
	if !errors.Is(err, ErrTerminating) {
		t.Errorf("expected ErrTerminating when adding finalizer to terminating record, got: %v", err)
	}
}

func TestService_UpdateRemoveLastFinalizer_TriggersHardDelete(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "ts-remove-last-finalizer",
			Finalizers: []string{"only.example.com"},
		},
		Spec: map[string]any{},
	}
	if _, err := svc.Create(ctx, r); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Soft-delete.
	if err := svc.Delete(ctx, "core/v1/Tradespace", "default", "ts-remove-last-finalizer"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Get the terminating record.
	terminating, err := svc.Get(ctx, "core/v1/Tradespace", "default", "ts-remove-last-finalizer")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !terminating.ObjectMeta.IsTerminating() {
		t.Fatal("expected record to be terminating")
	}

	// Remove the last finalizer.
	terminating.ObjectMeta.Finalizers = nil
	if _, err := svc.Update(ctx, terminating); err != nil {
		t.Fatalf("Update (remove last finalizer) failed: %v", err)
	}

	// Record must now be gone (hard delete triggered).
	_, err = svc.Get(ctx, "core/v1/Tradespace", "default", "ts-remove-last-finalizer")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound after removing last finalizer from terminating record, got: %v", err)
	}
}

func TestService_UpdateRemoveFinalizer_StillTerminating(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "ts-partial-finalizer",
			Finalizers: []string{"first.example.com", "second.example.com"},
		},
		Spec: map[string]any{},
	}
	if _, err := svc.Create(ctx, r); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Soft-delete.
	if err := svc.Delete(ctx, "core/v1/Tradespace", "default", "ts-partial-finalizer"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Get the terminating record and remove only one finalizer.
	terminating, err := svc.Get(ctx, "core/v1/Tradespace", "default", "ts-partial-finalizer")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	terminating.ObjectMeta.Finalizers = []string{"second.example.com"}
	updated, err := svc.Update(ctx, terminating)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Still terminating with one remaining finalizer.
	if !updated.ObjectMeta.IsTerminating() {
		t.Error("expected record to still be terminating")
	}
	if len(updated.ObjectMeta.Finalizers) != 1 || updated.ObjectMeta.Finalizers[0] != "second.example.com" {
		t.Errorf("unexpected finalizers: %v", updated.ObjectMeta.Finalizers)
	}

	// Still retrievable from storage.
	got, err := svc.Get(ctx, "core/v1/Tradespace", "default", "ts-partial-finalizer")
	if err != nil {
		t.Fatalf("record should still exist: %v", err)
	}
	if len(got.ObjectMeta.Finalizers) != 1 {
		t.Errorf("expected 1 finalizer in storage, got %d", len(got.ObjectMeta.Finalizers))
	}
}

func TestService_UpdateCanKeepFinalizersOnNonTerminating(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "ts-keep-finalizers",
			Finalizers: []string{"a.example.com"},
		},
		Spec: map[string]any{},
	}
	created, err := svc.Create(ctx, r)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Add another finalizer while the record is not yet terminating — must succeed.
	created.ObjectMeta.Finalizers = append(created.ObjectMeta.Finalizers, "b.example.com")
	updated, err := svc.Update(ctx, created)
	if err != nil {
		t.Fatalf("Update (add finalizer to non-terminating) failed: %v", err)
	}
	if len(updated.ObjectMeta.Finalizers) != 2 {
		t.Errorf("expected 2 finalizers, got %d", len(updated.ObjectMeta.Finalizers))
	}
}

func TestService_FinalizerRoundTrip_PreservesValues(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	finalizers := []string{"alpha.example.com", "beta.example.com", "gamma.example.com"}

	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "ts-finalizer-roundtrip",
			Finalizers: finalizers,
		},
		Spec: map[string]any{},
	}
	created, err := svc.Create(ctx, r)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	got, err := svc.Get(ctx, "core/v1/Tradespace", "default", "ts-finalizer-roundtrip")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(got.ObjectMeta.Finalizers) != len(finalizers) {
		t.Fatalf("expected %d finalizers, got %d", len(finalizers), len(got.ObjectMeta.Finalizers))
	}
	finalizerSet := make(map[string]bool)
	for _, f := range got.ObjectMeta.Finalizers {
		finalizerSet[f] = true
	}
	for _, f := range finalizers {
		if !finalizerSet[f] {
			t.Errorf("missing finalizer %q after round-trip", f)
		}
	}
	_ = created
}

// ---------------------------------------------------------------------------
// setDiff helper tests
// ---------------------------------------------------------------------------

func TestSetDiff(t *testing.T) {
	tests := []struct {
		name string
		a    []string
		b    []string
		want []string
	}{
		{
			name: "empty inputs",
			a:    nil,
			b:    nil,
			want: nil,
		},
		{
			name: "a is empty",
			a:    nil,
			b:    []string{"x"},
			want: nil,
		},
		{
			name: "b is empty — all of a returned",
			a:    []string{"x", "y"},
			b:    nil,
			want: []string{"x", "y"},
		},
		{
			name: "no difference",
			a:    []string{"x", "y"},
			b:    []string{"x", "y"},
			want: nil,
		},
		{
			name: "some difference",
			a:    []string{"x", "y", "z"},
			b:    []string{"x"},
			want: []string{"y", "z"},
		},
		{
			name: "b is superset of a",
			a:    []string{"x"},
			b:    []string{"x", "y"},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := setDiff(tt.a, tt.b)
			if len(got) != len(tt.want) {
				t.Fatalf("setDiff() = %v, want %v", got, tt.want)
			}
			wantSet := make(map[string]bool)
			for _, v := range tt.want {
				wantSet[v] = true
			}
			for _, v := range got {
				if !wantSet[v] {
					t.Errorf("unexpected element %q in result", v)
				}
			}
		})
	}
}

var counter int

func randomSuffix() string {
	counter++
	return string(rune('a' + counter%26))
}
