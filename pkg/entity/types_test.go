package entity

import "testing"

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
