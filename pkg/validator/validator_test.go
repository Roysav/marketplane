package validator

import (
	"context"
	"errors"
	"testing"

	"github.com/roysav/marketplane/pkg/entity"
	"github.com/roysav/marketplane/pkg/storage"
	"github.com/roysav/marketplane/pkg/storage/sqlite"
)

func newTestValidator(t *testing.T) (*Validator, *sqlite.Storage) {
	t.Helper()
	s, err := sqlite.New(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return New(s), s
}

func TestValidate_CoreTypes(t *testing.T) {
	v, _ := newTestValidator(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		entity  *entity.Entity
		wantErr bool
	}{
		{
			name: "valid Tradespace",
			entity: &entity.Entity{
				TypeMeta: entity.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
				Spec:     map[string]any{"description": "test"},
			},
			wantErr: false,
		},
		{
			name: "valid Quota",
			entity: &entity.Entity{
				TypeMeta: entity.TypeMeta{Group: "core", Version: "v1", Kind: "Quota"},
				Spec:     map[string]any{"balances": map[string]any{"USD": "1000"}},
			},
			wantErr: false,
		},
		{
			name: "invalid Quota - missing balances",
			entity: &entity.Entity{
				TypeMeta: entity.TypeMeta{Group: "core", Version: "v1", Kind: "Quota"},
				Spec:     map[string]any{},
			},
			wantErr: true,
		},
		{
			name: "valid EntityDefinition",
			entity: &entity.Entity{
				TypeMeta: entity.TypeMeta{Group: "core", Version: "v1", Kind: "EntityDefinition"},
				Spec: map[string]any{
					"group":   "test",
					"version": "v1",
					"kind":    "Foo",
					"storage": "record",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid EntityDefinition - bad storage enum",
			entity: &entity.Entity{
				TypeMeta: entity.TypeMeta{Group: "core", Version: "v1", Kind: "EntityDefinition"},
				Spec: map[string]any{
					"group":   "test",
					"version": "v1",
					"kind":    "Foo",
					"storage": "invalid",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(ctx, tt.entity)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_CustomType(t *testing.T) {
	v, s := newTestValidator(t)
	ctx := context.Background()

	// Create an EntityDefinition for a custom type
	_, err := s.Create(ctx, &storage.Record{
		Type:       "core/v1/EntityDefinition",
		Tradespace: "default",
		Name:       "Order.polymarket",
		Data: `{
			"group": "polymarket",
			"version": "v1",
			"kind": "Order",
			"storage": "record",
			"schema": {
				"type": "object",
				"required": ["marketId", "side"],
				"properties": {
					"marketId": {"type": "string"},
					"side": {"type": "string", "enum": ["YES", "NO"]},
					"amount": {"type": "string"}
				}
			}
		}`,
	})
	if err != nil {
		t.Fatalf("failed to create EntityDefinition: %v", err)
	}

	tests := []struct {
		name    string
		entity  *entity.Entity
		wantErr bool
	}{
		{
			name: "valid Order",
			entity: &entity.Entity{
				TypeMeta: entity.TypeMeta{Group: "polymarket", Version: "v1", Kind: "Order"},
				Spec: map[string]any{
					"marketId": "abc123",
					"side":     "YES",
					"amount":   "100",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid Order - missing required field",
			entity: &entity.Entity{
				TypeMeta: entity.TypeMeta{Group: "polymarket", Version: "v1", Kind: "Order"},
				Spec: map[string]any{
					"marketId": "abc123",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid Order - bad enum value",
			entity: &entity.Entity{
				TypeMeta: entity.TypeMeta{Group: "polymarket", Version: "v1", Kind: "Order"},
				Spec: map[string]any{
					"marketId": "abc123",
					"side":     "MAYBE",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(ctx, tt.entity)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_UnknownType(t *testing.T) {
	v, _ := newTestValidator(t)
	ctx := context.Background()

	e := &entity.Entity{
		TypeMeta: entity.TypeMeta{Group: "unknown", Version: "v1", Kind: "Thing"},
		Spec:     map[string]any{},
	}

	err := v.Validate(ctx, e)
	if !errors.Is(err, ErrUnknownType) {
		t.Errorf("expected ErrUnknownType, got: %v", err)
	}
}

func TestIsCoreType(t *testing.T) {
	tests := []struct {
		typeStr string
		want    bool
	}{
		{"core/v1/Tradespace", true},
		{"core/v1/Quota", true},
		{"core/v1/EntityDefinition", true},
		{"polymarket/v1/Order", false},
	}

	for _, tt := range tests {
		t.Run(tt.typeStr, func(t *testing.T) {
			if got := IsCoreType(tt.typeStr); got != tt.want {
				t.Errorf("IsCoreType() = %v, want %v", got, tt.want)
			}
		})
	}
}
