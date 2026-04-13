// Package record defines core record types.
package record

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

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
	StorageRow    StorageType = "record"
	StorageStream StorageType = "stream"
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
			"required": []any{"group", "version", "kind", "schema"},
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

const MetaRecordType = "core/v1/MetaRecord"

type Validator struct {
	mu          sync.RWMutex
	schemas     map[string]*gojsonschema.Schema
	definitions map[string][]byte
}

func NewValidator() (*Validator, error) {
	v := &Validator{
		schemas:     make(map[string]*gojsonschema.Schema),
		definitions: make(map[string][]byte),
	}

	for key, schema := range coreSchemas {
		prepared, err := prepareSchema(key, schema)
		if err != nil {
			return nil, err
		}
		v.schemas[key] = prepared.compiled
		v.definitions[key] = prepared.definition
	}

	return v, nil
}

func (v *Validator) CheckMetaRecord(r *Record) error {
	prepared, err := prepareMetaRecordSchema(r)
	if err != nil {
		return err
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	return ensureImmutable(v.definitions, prepared.typeStr, prepared.definition)
}

func (v *Validator) RegisterMetaRecord(r *Record) error {
	prepared, err := prepareMetaRecordSchema(r)
	if err != nil {
		return err
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if err := ensureImmutable(v.definitions, prepared.typeStr, prepared.definition); err != nil {
		return err
	}
	if _, exists := v.schemas[prepared.typeStr]; exists {
		return nil
	}

	v.schemas[prepared.typeStr] = prepared.compiled
	v.definitions[prepared.typeStr] = prepared.definition
	return nil
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
	errors := make([]error, 0)
	v.mu.RLock()
	jsonSchema, ok := v.schemas[t]
	v.mu.RUnlock()
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

func EnsureMetaRecordDefinitionImmutable(current, next *Record) error {
	currentType, currentDefinition, err := metaRecordFingerprint(current)
	if err != nil {
		return err
	}

	nextType, nextDefinition, err := metaRecordFingerprint(next)
	if err != nil {
		return err
	}

	if currentType != nextType {
		return fmt.Errorf("MetaRecord type is immutable: %s -> %s", currentType, nextType)
	}
	if !bytes.Equal(currentDefinition, nextDefinition) {
		return fmt.Errorf("schema for %s is immutable", currentType)
	}

	return nil
}

type preparedSchema struct {
	typeStr    string
	compiled   *gojsonschema.Schema
	definition []byte
}

func prepareMetaRecordSchema(r *Record) (*preparedSchema, error) {
	typeStr, schema, err := MetaRecordDefinition(r)
	if err != nil {
		return nil, err
	}

	return prepareSchema(typeStr, schema)
}

func prepareSchema(typeStr string, schema map[string]any) (*preparedSchema, error) {
	definition, err := normalizeSchema(schema)
	if err != nil {
		return nil, fmt.Errorf("normalize schema for %s: %w", typeStr, err)
	}

	compiled, err := gojsonschema.NewSchema(gojsonschema.NewGoLoader(schema))
	if err != nil {
		return nil, fmt.Errorf("compile schema for %s: %w", typeStr, err)
	}

	return &preparedSchema{
		typeStr:    typeStr,
		compiled:   compiled,
		definition: definition,
	}, nil
}

func ensureImmutable(definitions map[string][]byte, typeStr string, definition []byte) error {
	existing, ok := definitions[typeStr]
	if !ok {
		return nil
	}
	if bytes.Equal(existing, definition) {
		return nil
	}
	return fmt.Errorf("schema for %s is immutable", typeStr)
}

func MetaRecordDefinition(r *Record) (string, map[string]any, error) {
	rawSchema := r.Spec["schema"].(map[string]any)
	return fmt.Sprintf("%s/%s/%s", r.Spec["group"], r.Spec["version"], r.Spec["kind"]), rawSchema, nil
}

func metaRecordFingerprint(r *Record) (string, []byte, error) {
	typeStr, schema, err := MetaRecordDefinition(r)
	if err != nil {
		return "", nil, err
	}

	definition, err := normalizeSchema(schema)
	if err != nil {
		return "", nil, fmt.Errorf("normalize schema for %s: %w", typeStr, err)
	}

	return typeStr, definition, nil
}

func normalizeSchema(schema map[string]any) ([]byte, error) {
	return json.Marshal(schema)
}
