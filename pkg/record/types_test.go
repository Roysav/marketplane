package record

import (
	"testing"
	"time"
)

func TestGroupVersionKind_String(t *testing.T) {
	tests := []struct {
		gvk  GroupVersionKind
		want string
	}{
		{
			gvk:  GroupVersionKind{Group: "core", Version: "v1", Kind: "Tradespace"},
			want: "core/v1/Tradespace",
		},
		{
			gvk:  GroupVersionKind{Group: "polymarket", Version: "v1", Kind: "Order"},
			want: "polymarket/v1/Order",
		},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.gvk.String(); got != tt.want {
				t.Errorf("GVK.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTypeMeta_GVK(t *testing.T) {
	tm := TypeMeta{Group: "core", Version: "v1", Kind: "Quota"}
	gvk := tm.GVK()

	if gvk.Group != "core" || gvk.Version != "v1" || gvk.Kind != "Quota" {
		t.Errorf("GVK mismatch: got %+v", gvk)
	}
}

func TestObjectMeta_IsTerminating(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name string
		meta ObjectMeta
		want bool
	}{
		{
			name: "not terminating when DeletionTimestamp is nil and no finalizers",
			meta: ObjectMeta{},
			want: false,
		},
		{
			name: "not terminating when DeletionTimestamp is nil but finalizers present",
			meta: ObjectMeta{Finalizers: []string{"cleanup"}},
			want: false,
		},
		{
			name: "terminating when DeletionTimestamp is set and no finalizers",
			meta: ObjectMeta{DeletionTimestamp: &now},
			want: true,
		},
		{
			name: "terminating when DeletionTimestamp is set and finalizers present",
			meta: ObjectMeta{DeletionTimestamp: &now, Finalizers: []string{"cleanup"}},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.meta.IsTerminating(); got != tt.want {
				t.Errorf("IsTerminating() = %v, want %v", got, tt.want)
			}
		})
	}
}
