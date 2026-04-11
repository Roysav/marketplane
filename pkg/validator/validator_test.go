package validator

import (
	"context"
	"errors"
	"testing"

	"github.com/roysav/marketplane/pkg/record"
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
		record  *record.Record
		wantErr bool
	}{
		{
			name: "valid Tradespace",
			record: &record.Record{
				TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Tradespace"},
				Spec:     map[string]any{"description": "test"},
			},
			wantErr: false,
		},
		{
			name: "valid Quota",
			record: &record.Record{
				TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Quota"},
				Spec:     map[string]any{"balances": map[string]any{"USD": "1000"}},
			},
			wantErr: false,
		},
		{
			name: "invalid Quota - missing balances",
			record: &record.Record{
				TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Quota"},
				Spec:     map[string]any{},
			},
			wantErr: true,
		},
		{
			name: "valid MetaRecord",
			record: &record.Record{
				TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "MetaRecord"},
				Spec: map[string]any{
					"group":   "test",
					"version": "v1",
					"kind":    "Foo",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid MetaRecord - missing kind",
			record: &record.Record{
				TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "MetaRecord"},
				Spec: map[string]any{
					"group":   "test",
					"version": "v1",
				},
			},
			wantErr: true,
		},
		{
			name: "valid StreamDefinition",
			record: &record.Record{
				TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "StreamDefinition"},
				Spec: map[string]any{
					"group":     "binance",
					"version":   "v1",
					"kind":      "Price",
					"retention": "24h",
				},
			},
			wantErr: false,
		},
		{
			name: "valid StreamDefinition with schema",
			record: &record.Record{
				TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "StreamDefinition"},
				Spec: map[string]any{
					"group":   "binance",
					"version": "v1",
					"kind":    "Price",
					"schema": map[string]any{
						"type":     "object",
						"required": []any{"last_price"},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid StreamDefinition - missing group",
			record: &record.Record{
				TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "StreamDefinition"},
				Spec: map[string]any{
					"version": "v1",
					"kind":    "Price",
				},
			},
			wantErr: true,
		},
		{
			name: "valid User",
			record: &record.Record{
				TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "User"},
				Spec:     map[string]any{"commonName": "alice", "description": "test user"},
			},
			wantErr: false,
		},
		{
			name: "valid User - minimal",
			record: &record.Record{
				TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "User"},
				Spec:     map[string]any{"commonName": "alice"},
			},
			wantErr: false,
		},
		{
			name: "invalid User - missing commonName",
			record: &record.Record{
				TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "User"},
				Spec:     map[string]any{"description": "no cn"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(ctx, tt.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_CustomType(t *testing.T) {
	v, s := newTestValidator(t)
	ctx := context.Background()

	// Create a MetaRecord for a custom type
	_, err := s.Create(ctx, &storage.Row{
		Type:       "core/v1/MetaRecord",
		Tradespace: "default",
		Name:       "Order.polymarket",
		Data: `{
			"group": "polymarket",
			"version": "v1",
			"kind": "Order",
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
		t.Fatalf("failed to create MetaRecord: %v", err)
	}

	tests := []struct {
		name    string
		record  *record.Record
		wantErr bool
	}{
		{
			name: "valid Order",
			record: &record.Record{
				TypeMeta: record.TypeMeta{Group: "polymarket", Version: "v1", Kind: "Order"},
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
			record: &record.Record{
				TypeMeta: record.TypeMeta{Group: "polymarket", Version: "v1", Kind: "Order"},
				Spec: map[string]any{
					"marketId": "abc123",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid Order - bad enum value",
			record: &record.Record{
				TypeMeta: record.TypeMeta{Group: "polymarket", Version: "v1", Kind: "Order"},
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
			err := v.Validate(ctx, tt.record)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidate_UnknownType(t *testing.T) {
	v, _ := newTestValidator(t)
	ctx := context.Background()

	r := &record.Record{
		TypeMeta: record.TypeMeta{Group: "unknown", Version: "v1", Kind: "Thing"},
		Spec:     map[string]any{},
	}

	err := v.Validate(ctx, r)
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
		{"core/v1/MetaRecord", true},
		{"core/v1/StreamDefinition", true},
		{"core/v1/User", true},
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

func TestValidateScope_User(t *testing.T) {
	v, _ := newTestValidator(t)
	ctx := context.Background()

	tests := []struct {
		name       string
		tradespace string
		wantErr    bool
	}{
		{
			name:       "tradespace scope - specific tradespace accepted",
			tradespace: "my-tradespace",
			wantErr:    false,
		},
		{
			name:       "tradespace scope - empty tradespace rejected",
			tradespace: "",
			wantErr:    true,
		},
		{
			name:       "tradespace scope - default tradespace rejected",
			tradespace: "default",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &record.Record{
				TypeMeta:   record.TypeMeta{Group: "core", Version: "v1", Kind: "User"},
				ObjectMeta: record.ObjectMeta{Tradespace: tt.tradespace},
				Spec:       map[string]any{"commonName": "alice"},
			}
			err := v.ValidateScope(ctx, r)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateScope() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
