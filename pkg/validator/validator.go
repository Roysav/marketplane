// Package validator validates records against their schemas.
package validator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/xeipuuv/gojsonschema"

	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/storage"
)

var (
	ErrUnknownType = errors.New("unknown record type")
	ErrValidation  = errors.New("validation failed")
)

// coreSchemas holds JSON Schema definitions for built-in types.
var coreSchemas = map[string]map[string]any{
	"core/v1/MetaRecord": {
		"type":     "object",
		"required": []any{"group", "version", "kind"},
		"properties": map[string]any{
			"group":   map[string]any{"type": "string"},
			"version": map[string]any{"type": "string"},
			"kind":    map[string]any{"type": "string"},
			"scope":   map[string]any{"type": "string", "enum": []any{"tradespace", "global"}},
			"schema":  map[string]any{"type": "object"},
		},
	},
	"core/v1/StreamDefinition": {
		"type":     "object",
		"required": []any{"group", "version", "kind"},
		"properties": map[string]any{
			"group":     map[string]any{"type": "string"},
			"version":   map[string]any{"type": "string"},
			"kind":      map[string]any{"type": "string"},
			"schema":    map[string]any{"type": "object"},
			"retention": map[string]any{"type": "string"},
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

// compiledCoreSchemas holds pre-compiled schemas for core types.
var compiledCoreSchemas map[string]*gojsonschema.Schema

func init() {
	compiledCoreSchemas = make(map[string]*gojsonschema.Schema)
	for typeStr, schemaData := range coreSchemas {
		schema, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(schemaData))
		if err != nil {
			panic(fmt.Sprintf("failed to compile core schema %s: %v", typeStr, err))
		}
		compiledCoreSchemas[typeStr] = schema
	}
}

// Validator validates records against their schemas.
type Validator struct {
	storage storage.RowStorage
}

// New creates a new Validator.
func New(s storage.RowStorage) *Validator {
	return &Validator{storage: s}
}

// Validate checks a record's spec against its schema.
func (v *Validator) Validate(ctx context.Context, r *record.Record) error {
	typeStr := r.TypeMeta.GVK().Type()

	// Check core types first
	if schema, ok := compiledCoreSchemas[typeStr]; ok {
		return validateWithSchema(schema, r.Spec)
	}

	// Look up MetaRecord from storage
	row, err := v.storage.Get(ctx, storage.Key{
		Type:       "core/v1/MetaRecord",
		Tradespace: "default",
		Name:       definitionName(r.TypeMeta.GVK()),
	})
	if err != nil {
		return fmt.Errorf("%w: %s", ErrUnknownType, typeStr)
	}

	// Parse the MetaRecord data
	var data map[string]any
	if err := json.Unmarshal([]byte(row.Data), &data); err != nil {
		return fmt.Errorf("invalid MetaRecord data: %w", err)
	}

	schemaData, ok := data["schema"].(map[string]any)
	if !ok || schemaData == nil {
		return nil // No schema defined, allow anything
	}

	schema, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(schemaData))
	if err != nil {
		return fmt.Errorf("invalid schema for %s: %w", typeStr, err)
	}

	return validateWithSchema(schema, r.Spec)
}

// IsCoreType returns true if the type string is a built-in core type.
func IsCoreType(typeStr string) bool {
	_, ok := coreSchemas[typeStr]
	return ok
}

// definitionName returns the MetaRecord name for a GVK.
func definitionName(gvk record.GroupVersionKind) string {
	return fmt.Sprintf("%s.%s", gvk.Kind, gvk.Group)
}

func validateWithSchema(schema *gojsonschema.Schema, spec map[string]any) error {
	result, err := schema.Validate(gojsonschema.NewGoLoader(spec))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrValidation, err)
	}

	if !result.Valid() {
		var msgs []string
		for _, e := range result.Errors() {
			msgs = append(msgs, e.String())
		}
		return fmt.Errorf("%w: %s", ErrValidation, strings.Join(msgs, "; "))
	}

	return nil
}
