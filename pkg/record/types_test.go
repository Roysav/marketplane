package record

import "testing"

func TestValidatorRegisterMetaRecordRejectsSchemaChanges(t *testing.T) {
	validator, err := NewValidator()
	if err != nil {
		t.Fatalf("NewValidator failed: %v", err)
	}

	original := &Record{
		Type: MetaRecordType,
		ObjectMeta: ObjectMeta{
			Tradespace: "default",
			Name:       "order-a",
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

	changed := &Record{
		Type: MetaRecordType,
		ObjectMeta: ObjectMeta{
			Tradespace: "default",
			Name:       "order-b",
		},
		Spec: map[string]any{
			"group":   "polymarket",
			"version": "v1",
			"kind":    "Order",
			"schema": map[string]any{
				"type":     "object",
				"required": []any{"marketId", "side"},
				"properties": map[string]any{
					"marketId": map[string]any{"type": "string"},
					"side":     map[string]any{"type": "string"},
				},
			},
		},
	}

	if err := validator.RegisterMetaRecord(original); err != nil {
		t.Fatalf("RegisterMetaRecord(original) failed: %v", err)
	}

	validOrder := &Record{
		Type: "polymarket/v1/Order",
		Spec: map[string]any{"marketId": "abc123"},
	}
	if err := validator.Validate(validOrder); err != nil {
		t.Fatalf("Validate(validOrder) failed: %v", err)
	}

	if err := validator.RegisterMetaRecord(changed); err == nil {
		t.Fatal("expected schema immutability error")
	}
}
