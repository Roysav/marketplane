// Package entity defines core entity types.
package entity

import (
	"fmt"
	"time"
)

// GroupVersionKind uniquely identifies an entity type.
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

// TypeMeta describes the type of an entity.
type TypeMeta struct {
	Group   string `json:"group"`
	Version string `json:"version"`
	Kind    string `json:"kind"`
}

func (t TypeMeta) GVK() GroupVersionKind {
	return GroupVersionKind{Group: t.Group, Version: t.Version, Kind: t.Kind}
}

// ObjectMeta contains metadata about an entity.
type ObjectMeta struct {
	Name            string            `json:"name"`
	Tradespace      string            `json:"tradespace,omitempty"`
	UID             string            `json:"uid,omitempty"`
	ResourceVersion int64             `json:"resourceVersion,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	Annotations     map[string]string `json:"annotations,omitempty"`
	CreatedAt       time.Time         `json:"createdAt,omitempty"`
	UpdatedAt       time.Time         `json:"updatedAt,omitempty"`
	DeletedAt       *time.Time        `json:"deletedAt,omitempty"`
}

// Entity is the generic wrapper for all resources.
type Entity struct {
	TypeMeta   `json:",inline"`
	ObjectMeta `json:"metadata"`
	Spec       map[string]any `json:"spec,omitempty"`
	Status     map[string]any `json:"status,omitempty"`
}

// StorageType indicates where entities are stored.
type StorageType string

const (
	StorageRecord StorageType = "record" // PostgreSQL
	StorageStream StorageType = "stream" // Redis
)

// ScopeType indicates entity scoping.
type ScopeType string

const (
	ScopeTradespace ScopeType = "tradespace" // Belongs to a tradespace
	ScopeGlobal     ScopeType = "global"     // Cluster-wide
)
