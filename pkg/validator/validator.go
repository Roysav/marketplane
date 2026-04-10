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

// Scope defines where a record type can be created.
type Scope string

const (
	ScopeTradespace Scope = "tradespace" // Must have a specific tradespace
	ScopeGlobal     Scope = "global"     // Must use "default" tradespace
)

var (
	ErrUnknownType  = errors.New("unknown record type")
	ErrValidation   = errors.New("validation failed")
	ErrInvalidScope = errors.New("invalid scope")
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
			"scope":     map[string]any{"type": "string", "enum": []any{"tradespace", "global"}},
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
	"core/v1/Allocation": {
		"type":     "object",
		"required": []any{"currency", "amount", "targetType", "targetName"},
		"properties": map[string]any{
			"currency":   map[string]any{"type": "string"},
			"amount":     map[string]any{"type": "string"}, // decimal string, negative for spending
			"targetType": map[string]any{"type": "string"}, // e.g., "polymarket/v1/Order"
			"targetName": map[string]any{"type": "string"}, // name of the target record
		},
	},
}

// coreScopes defines the scope for built-in types.
var coreScopes = map[string]Scope{
	"core/v1/MetaRecord":       ScopeGlobal,     // Type definitions are global
	"core/v1/StreamDefinition": ScopeGlobal,     // Stream definitions are global
	"core/v1/Tradespace":       ScopeGlobal,     // Tradespaces themselves are global
	"core/v1/Quota":            ScopeTradespace, // Quotas are per-tradespace
	"core/v1/Allocation":       ScopeTradespace, // Allocations are per-tradespace
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

// ValidateScope checks that the record's tradespace matches its type's scope.
func (v *Validator) ValidateScope(ctx context.Context, r *record.Record) error {
	typeStr := r.TypeMeta.GVK().Type()
	tradespace := r.ObjectMeta.Tradespace

	scope, err := v.GetScope(ctx, typeStr)
	if err != nil {
		return err
	}

	switch scope {
	case ScopeGlobal:
		if tradespace != "" && tradespace != "default" {
			return fmt.Errorf("%w: type %s is global, tradespace must be 'default' or empty, got '%s'",
				ErrInvalidScope, typeStr, tradespace)
		}
	case ScopeTradespace:
		if tradespace == "" || tradespace == "default" {
			return fmt.Errorf("%w: type %s is tradespace-scoped, tradespace is required",
				ErrInvalidScope, typeStr)
		}
	}

	return nil
}

// GetScope returns the scope for a record type.
func (v *Validator) GetScope(ctx context.Context, typeStr string) (Scope, error) {
	// Check core types first
	if scope, ok := coreScopes[typeStr]; ok {
		return scope, nil
	}

	// Look up MetaRecord from storage
	row, err := v.storage.Get(ctx, storage.Key{
		Type:       "core/v1/MetaRecord",
		Tradespace: "default",
		Name:       definitionNameFromType(typeStr),
	})
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrUnknownType, typeStr)
	}

	// Parse the MetaRecord data
	var data map[string]any
	if err := json.Unmarshal([]byte(row.Data), &data); err != nil {
		return "", fmt.Errorf("invalid MetaRecord data: %w", err)
	}

	// Default to tradespace scope if not specified
	scopeStr, _ := data["scope"].(string)
	if scopeStr == "" {
		return ScopeTradespace, nil
	}

	return Scope(scopeStr), nil
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

// definitionNameFromType returns the MetaRecord name from a type string.
func definitionNameFromType(typeStr string) string {
	parts := strings.Split(typeStr, "/")
	if len(parts) != 3 {
		return typeStr
	}
	return fmt.Sprintf("%s.%s", parts[2], parts[0]) // Kind.Group
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
