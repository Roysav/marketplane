// Package validator validates entities against their schemas.
package validator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/roysav/marketplane/pkg/entity"
	"github.com/roysav/marketplane/pkg/storage"
)

var (
	ErrUnknownType      = errors.New("unknown entity type")
	ErrMissingField     = errors.New("missing required field")
	ErrInvalidType      = errors.New("invalid field type")
	ErrInvalidEnumValue = errors.New("invalid enum value")
)

// coreSchemas holds schemas for built-in types.
var coreSchemas = map[string]map[string]any{
	"core/v1/EntityDefinition": {
		"type":     "object",
		"required": []any{"group", "version", "kind", "storage"},
		"properties": map[string]any{
			"group":   map[string]any{"type": "string"},
			"version": map[string]any{"type": "string"},
			"kind":    map[string]any{"type": "string"},
			"storage": map[string]any{"type": "string", "enum": []any{"record", "stream"}},
			"scope":   map[string]any{"type": "string", "enum": []any{"tradespace", "global"}},
			"schema":  map[string]any{"type": "object"},
		},
	},
	"core/v1/Tradespace": {
		"type": "object",
		"properties": map[string]any{
			"description": map[string]any{"type": "string"},
		},
	},
	"core/v1/Quota": {
		"type":     "object",
		"required": []any{"balances"},
		"properties": map[string]any{
			"balances": map[string]any{
				"type":                 "object",
				"additionalProperties": map[string]any{"type": "string"},
			},
		},
	},
}

// Validator validates entities against their schemas.
type Validator struct {
	storage storage.RecordStorage
}

// New creates a new Validator.
func New(s storage.RecordStorage) *Validator {
	return &Validator{storage: s}
}

// Validate checks an entity's spec against its schema.
func (v *Validator) Validate(ctx context.Context, e *entity.Entity) error {
	typeStr := e.TypeMeta.GVK().Type()

	// Check core types first
	if schema, ok := coreSchemas[typeStr]; ok {
		return validateSpec(e.Spec, schema)
	}

	// Look up EntityDefinition from storage
	record, err := v.storage.Get(ctx, storage.Key{
		Type:       "core/v1/EntityDefinition",
		Tradespace: "default",
		Name:       definitionName(e.TypeMeta.GVK()),
	})
	if err != nil {
		return fmt.Errorf("%w: %s", ErrUnknownType, typeStr)
	}

	// Parse the EntityDefinition data
	var data map[string]any
	if err := json.Unmarshal([]byte(record.Data), &data); err != nil {
		return fmt.Errorf("invalid EntityDefinition data: %w", err)
	}

	schema, _ := data["schema"].(map[string]any)
	if schema == nil {
		return nil
	}

	return validateSpec(e.Spec, schema)
}

// IsCoreType returns true if the type string is a built-in core type.
func IsCoreType(typeStr string) bool {
	_, ok := coreSchemas[typeStr]
	return ok
}

// definitionName returns the EntityDefinition name for a GVK.
func definitionName(gvk entity.GroupVersionKind) string {
	return fmt.Sprintf("%s.%s", gvk.Kind, gvk.Group)
}

func validateSpec(spec map[string]any, schema map[string]any) error {
	if required, ok := schema["required"].([]any); ok {
		for _, r := range required {
			field := r.(string)
			if _, exists := spec[field]; !exists {
				return fmt.Errorf("%w: %s", ErrMissingField, field)
			}
		}
	}

	properties, _ := schema["properties"].(map[string]any)
	for field, value := range spec {
		propSchema, ok := properties[field].(map[string]any)
		if !ok {
			continue
		}
		if err := validateType(field, value, propSchema); err != nil {
			return err
		}
	}

	return nil
}

func validateType(field string, value any, schema map[string]any) error {
	expectedType, _ := schema["type"].(string)

	switch expectedType {
	case "string":
		strVal, ok := value.(string)
		if !ok {
			return fmt.Errorf("%w: field %s expected string, got %T", ErrInvalidType, field, value)
		}
		if enum, ok := schema["enum"].([]any); ok {
			valid := false
			for _, e := range enum {
				if e.(string) == strVal {
					valid = true
					break
				}
			}
			if !valid {
				return fmt.Errorf("%w: field %s value %q", ErrInvalidEnumValue, field, strVal)
			}
		}
	case "object":
		if _, ok := value.(map[string]any); !ok {
			return fmt.Errorf("%w: field %s expected object, got %T", ErrInvalidType, field, value)
		}
	case "array":
		if _, ok := value.([]any); !ok {
			return fmt.Errorf("%w: field %s expected array, got %T", ErrInvalidType, field, value)
		}
	case "number":
		switch value.(type) {
		case float64, int, int64:
		default:
			return fmt.Errorf("%w: field %s expected number, got %T", ErrInvalidType, field, value)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%w: field %s expected boolean, got %T", ErrInvalidType, field, value)
		}
	}

	return nil
}
