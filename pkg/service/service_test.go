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

func TestService_CreateCustomTypeFromMetaRecord(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	_, err := svc.Create(ctx, &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "MetaRecord"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "CryptoSubscription.polymarket",
		},
		Spec: map[string]any{
			"group":   "polymarket",
			"version": "v1",
			"kind":    "CryptoSubscription",
			"scope":   "global",
			"schema": map[string]any{
				"type":     "object",
				"required": []any{"venue", "symbol"},
				"properties": map[string]any{
					"venue":   map[string]any{"type": "string"},
					"symbol":  map[string]any{"type": "string"},
					"enabled": map[string]any{"type": "boolean"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to create MetaRecord through service: %v", err)
	}

	created, err := svc.Create(ctx, &record.Record{
		TypeMeta: record.TypeMeta{Group: "polymarket", Version: "v1", Kind: "CryptoSubscription"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "polymarket.crypto.binance.btcusdt",
		},
		Spec: map[string]any{
			"venue":   "binance",
			"symbol":  "btcusdt",
			"enabled": true,
		},
	})
	if err != nil {
		t.Fatalf("failed to create custom record through service: %v", err)
	}

	if created.ObjectMeta.Name != "polymarket.crypto.binance.btcusdt" {
		t.Fatalf("unexpected custom record name: %s", created.ObjectMeta.Name)
	}
}

func TestService_MetaRecordIsImmutable(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	meta, err := svc.Create(ctx, &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "MetaRecord"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "Thing.example",
		},
		Spec: map[string]any{
			"group":   "example",
			"version": "v1",
			"kind":    "Thing",
		},
	})
	if err != nil {
		t.Fatalf("failed to create MetaRecord: %v", err)
	}

	meta.Spec["scope"] = "global"
	_, err = svc.Update(ctx, meta)
	if !errors.Is(err, ErrImmutable) {
		t.Fatalf("expected ErrImmutable on MetaRecord update, got %v", err)
	}

	err = svc.Delete(ctx, "core/v1/MetaRecord", "default", "Thing.example")
	if !errors.Is(err, ErrImmutable) {
		t.Fatalf("expected ErrImmutable on MetaRecord delete, got %v", err)
	}
}

func TestService_StreamDefinitionIsImmutable(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	def, err := svc.Create(ctx, &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "StreamDefinition"},
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       "prices.example",
		},
		Spec: map[string]any{
			"group":     "example",
			"version":   "v1",
			"kind":      "Price",
			"retention": "24h",
		},
	})
	if err != nil {
		t.Fatalf("failed to create StreamDefinition: %v", err)
	}

	def.Spec["retention"] = "48h"
	_, err = svc.Update(ctx, def)
	if !errors.Is(err, ErrImmutable) {
		t.Fatalf("expected ErrImmutable on StreamDefinition update, got %v", err)
	}

	err = svc.Delete(ctx, "core/v1/StreamDefinition", "default", "prices.example")
	if !errors.Is(err, ErrImmutable) {
		t.Fatalf("expected ErrImmutable on StreamDefinition delete, got %v", err)
	}
}

var counter int

func randomSuffix() string {
	counter++
	return string(rune('a' + counter%26))
}
