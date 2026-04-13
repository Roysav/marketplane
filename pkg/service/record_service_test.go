package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/service"
	"github.com/roysav/marketplane/pkg/storage/postgres"
	"github.com/roysav/marketplane/tests"
)

func newTestService(t *testing.T) *service.RecordService {
	t.Helper()
	ctx := context.Background()
	return tests.SVC(ctx, t)
}

func tradespaceRecord(name string) *record.Record {
	return &record.Record{
		Type: "core/v1/Tradespace",
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       name,
			Labels:     map[string]string{"env": "test"},
		},
		Spec: map[string]any{"description": "A test tradespace", "name": name},
	}
}

func metaRecord(name string) *record.Record {
	return &record.Record{
		Type: record.MetaRecordType,
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       name,
		},
		Spec: map[string]any{
			"group":   "polymarket",
			"version": "v1",
			"kind":    "Order",
			"schema": map[string]any{
				"type":     "object",
				"required": []any{"marketId"},
				"properties": map[string]any{
					"marketId": map[string]any{"type": "string"},
				},
			},
		},
	}
}

func customOrder(name string) *record.Record {
	return &record.Record{
		Type: "polymarket/v1/Order",
		ObjectMeta: record.ObjectMeta{
			Tradespace: "default",
			Name:       name,
		},
		Spec: map[string]any{
			"marketId": "market-123",
		},
	}
}

func TestService_CreateAndGet(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	r := tradespaceRecord("test-tradespace")

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

	r := tradespaceRecord("dup-tradespace")

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

	r := tradespaceRecord("update-test")

	created, err := svc.Create(ctx, r)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Update
	lastApplied, _ := json.Marshal(created)
	created.Spec["description"] = "updated"
	updated, err := svc.Update(ctx, created, lastApplied)
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

	r := tradespaceRecord("delete-test")

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
		r := tradespaceRecord(name)
		_, err := svc.Create(ctx, r)
		if err != nil {
			t.Fatalf("Create %s failed: %v", name, err)
		}
	}

	// List all
	records, err := svc.List(ctx, "core/v1/Tradespace", "default", nil)
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
	tradespace := "list-test"

	// Create records in different tradespaces
	for _, ts := range []string{"list-test-a", "list-test-b", "list-test-c"} {
		r := tradespaceRecord(ts)
		r.ObjectMeta.Tradespace = tradespace
		_, err := svc.Create(ctx, r)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
	}

	// List only prod
	records, err := svc.List(ctx, "core/v1/Tradespace", tradespace, nil)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(records) != 3 {
		t.Errorf("expected 3 records in prod, got %d", len(records))
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

func TestService_CreateMetaRecordRegistersSchema(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	if _, err := svc.Create(ctx, metaRecord("orders")); err != nil {
		t.Fatalf("Create MetaRecord failed: %v", err)
	}

	if _, err := svc.Create(ctx, customOrder("order-1")); err != nil {
		t.Fatalf("Create custom record failed: %v", err)
	}
}

func TestService_UpdateMetaRecordRejectsSchemaChange(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	created, err := svc.Create(ctx, metaRecord("orders"))
	if err != nil {
		t.Fatalf("Create MetaRecord failed: %v", err)
	}

	lastApplied, _ := json.Marshal(created)
	created.Spec["schema"] = map[string]any{
		"type":     "object",
		"required": []any{"marketId", "side"},
		"properties": map[string]any{
			"marketId": map[string]any{"type": "string"},
			"side":     map[string]any{"type": "string"},
		},
	}

	_, err = svc.Update(ctx, created, lastApplied)
	if !errors.Is(err, service.ErrValidation) {
		t.Fatalf("expected ErrValidation, got: %v", err)
	}
}

func TestService_SyncMetaRecordsLoadsExistingSchemas(t *testing.T) {
	ctx := context.Background()
	pool := tests.Pool(ctx, t)
	rows := postgres.New(pool)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	validator1, err := record.NewValidator()
	if err != nil {
		t.Fatalf("NewValidator failed: %v", err)
	}
	svc1 := service.New(service.Config{
		Rows:      rows,
		Validator: validator1,
		Logger:    logger,
	})

	if _, err := svc1.Create(ctx, metaRecord("orders")); err != nil {
		t.Fatalf("Create MetaRecord failed: %v", err)
	}

	validator2, err := record.NewValidator()
	if err != nil {
		t.Fatalf("NewValidator failed: %v", err)
	}
	svc2 := service.New(service.Config{
		Rows:      rows,
		Validator: validator2,
		Logger:    logger,
	})

	if _, err := svc2.Create(ctx, customOrder("before-sync")); !errors.Is(err, service.ErrValidation) {
		t.Fatalf("expected custom record create to fail before sync, got: %v", err)
	}

	if err := svc2.SyncMetaRecords(ctx); err != nil {
		t.Fatalf("SyncMetaRecords failed: %v", err)
	}

	if _, err := svc2.Create(ctx, customOrder("after-sync")); err != nil {
		t.Fatalf("expected custom record create to succeed after sync, got: %v", err)
	}
}

var counter int

func randomSuffix() string {
	counter++
	return string(rune('a' + counter%26))
}
