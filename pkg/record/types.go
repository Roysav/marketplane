// Package record defines core record types.
package record

import (
	"fmt"
	"time"
)

// GroupVersionKind uniquely identifies a record type.
type GroupVersionKind struct {
	Group   string
	Version string
	Kind    string
}

func (gvk GroupVersionKind) String() string {
	return fmt.Sprintf("%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind)
}

// Type returns the type string used in storage.
func (gvk GroupVersionKind) Type() string {
	return gvk.String()
}

// TypeMeta describes the type of a record.
type TypeMeta struct {
	Group   string `json:"group"`
	Version string `json:"version"`
	Kind    string `json:"kind"`
}

func (t TypeMeta) GVK() GroupVersionKind {
	return GroupVersionKind{Group: t.Group, Version: t.Version, Kind: t.Kind}
}

// TypeMetaFromType parses a type string like "core/v1/Tradespace" into TypeMeta.
func TypeMetaFromType(typeStr string) TypeMeta {
	gvk := ParseType(typeStr)
	return TypeMeta{Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind}
}

// ParseType parses a type string like "core/v1/Tradespace" into GroupVersionKind.
func ParseType(typeStr string) GroupVersionKind {
	var group, version, kind string
	parts := splitType(typeStr)
	if len(parts) >= 3 {
		group, version, kind = parts[0], parts[1], parts[2]
	}
	return GroupVersionKind{Group: group, Version: version, Kind: kind}
}

func splitType(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// ObjectMeta contains metadata about a record.
type ObjectMeta struct {
	Name              string            `json:"name"`
	Tradespace        string            `json:"tradespace,omitempty"`
	ResourceVersion   int64             `json:"resourceVersion,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	CreatedAt         time.Time         `json:"createdAt,omitempty"`
	UpdatedAt         time.Time         `json:"updatedAt,omitempty"`
	DeletionTimestamp *time.Time        `json:"deletionTimestamp,omitempty"`
	Finalizers        []string          `json:"finalizers,omitempty"`
}

// IsTerminating returns true if deletion has been requested but finalizers are still pending.
func (m *ObjectMeta) IsTerminating() bool {
	return m.DeletionTimestamp != nil
}

// Record is the generic wrapper for all resources.
type Record struct {
	TypeMeta   `json:",inline"`
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

const (
	ScopeTradespace ScopeType = "tradespace" // Belongs to a tradespace
	ScopeGlobal     ScopeType = "global"     // Cluster-wide
)
