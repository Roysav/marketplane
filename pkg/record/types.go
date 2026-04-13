// Package record defines core record types.
package record

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/roysav/marketplane/pkg/storage"
	"github.com/xeipuuv/gojsonschema"
)

type ObjectMeta struct {
	Name       string            `json:"name"`
	Tradespace string            `json:"tradespace,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
}

// Record is the generic wrapper for all resources.
type Record struct {
	Type       string `json:"type"`
	ObjectMeta `json:"metadata"`
	Spec       map[string]any `json:"spec,omitempty"`
	Status     map[string]any `json:"status,omitempty"`
}

// StorageType indicates where records are stored.
type StorageType string

const (
	StorageRow    StorageType = "record" // PostgreSQL/SQLite
	StorageStream StorageType = "stream" // Redis
)

// ScopeType indicates record scoping.
type ScopeType string

func (r *Record) Key() string {
	return Key(r.Type, r.Tradespace, r.Name)
}

func RecordFromRow(row *storage.Row) (*Record, error) {
	record := &Record{}
	err := json.Unmarshal(row.Data, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func Key(typeStr, tradespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", typeStr, tradespace, name)
}

func (r *Record) ToRow() (*storage.Row, error) {
	jsonData, err := json.Marshal(&r)
	if err != nil {
		return nil, err
	}
	return &storage.Row{
		Key:    r.Key(),
		Labels: r.Labels,
		Data:   jsonData,
	}, nil
}

var (
	// coreSchemas holds JSON Schema definitions for built-in types.
	coreSchemas = map[string]map[string]any{
		"core/v1/MetaRecord": {
			"type":     "object",
			"required": []any{"group", "version", "kind"},
			"properties": map[string]any{
				"group":   map[string]any{"type": "string"},
				"version": map[string]any{"type": "string"},
				"kind":    map[string]any{"type": "string"},
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
			"type":     "object",
			"required": []any{"name"},
			"properties": map[string]any{
				"name":        map[string]any{"type": "string"},
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
)

type Validator struct {
	Schemas map[string]*gojsonschema.Schema
}

func NewValidator() (*Validator, error) {
	compiledSchemas := make(map[string]*gojsonschema.Schema)
	for key, schema := range coreSchemas {
		compiled, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(schema))
		if err != nil {
			return nil, err
		}
		compiledSchemas[key] = compiled
	}

	return &Validator{
		Schemas: compiledSchemas,
	}, nil
}

func (v *Validator) Validate(r *Record) error {
	isValid, reasons, err := v.validate(r)
	if !isValid {
		reasons := append(reasons, err)
		return errors.Join(reasons...)
	}
	return nil
}

func (v *Validator) validate(r *Record) (bool, []error, error) {
	t := r.Type
	jsonSchema, ok := v.Schemas[t]
	errors := make([]error, 0)
	if !ok {
		return false, errors, fmt.Errorf("couldn't find schema for record of type %s", t)
	}
	recordLoader := gojsonschema.NewGoLoader(r.Spec)
	result, err := jsonSchema.Validate(recordLoader)
	if err != nil {
		return false, errors, err
	}
	isValid := result.Valid()
	if !isValid {
		for _, err := range result.Errors() {
			errors = append(errors, fmt.Errorf("%v", err))
		}
	}
	return isValid, errors, nil
}
