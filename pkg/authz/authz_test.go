package authz

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"testing"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/roysav/marketplane/pkg/auth"
	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/storage"
	"github.com/roysav/marketplane/pkg/storage/postgres"
)

const testDSN = "postgres://marketplane:marketplane@localhost:5432/marketplane?sslmode=disable"

func newTestAuthorizer(t *testing.T, bootstrap ...string) (*Authorizer, storage.RowStorage) {
	t.Helper()
	ctx := context.Background()

	rows, err := postgres.New(ctx, testDSN)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	rows.DB().ExecContext(ctx, "TRUNCATE records CASCADE")
	t.Cleanup(func() { rows.Close() })

	return New(Config{
		Rows:              rows,
		Enabled:           true,
		BootstrapAdminCNs: bootstrap,
	}), rows
}

func seedSpecRecord(t *testing.T, rows storage.RowStorage, typeStr, name string, spec any) {
	t.Helper()

	payload, err := json.Marshal(map[string]any{
		"spec":   spec,
		"status": map[string]any{},
	})
	if err != nil {
		t.Fatalf("failed to marshal test record: %v", err)
	}

	_, err = rows.Create(context.Background(), &storage.Row{
		Type:       typeStr,
		Tradespace: "default",
		Name:       name,
		Data:       string(payload),
	})
	if err != nil {
		t.Fatalf("failed to seed %s/%s: %v", typeStr, name, err)
	}
}

func contextWithCN(t *testing.T, cn string) context.Context {
	t.Helper()

	base := peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{
			State: tls.ConnectionState{
				VerifiedChains: [][]*x509.Certificate{{
					{Subject: pkix.Name{CommonName: cn}},
				}},
			},
		},
	})

	var out context.Context
	handler := func(ctx context.Context, _ any) (any, error) {
		out = ctx
		return nil, nil
	}
	if _, err := auth.UnaryInterceptor(base, nil, nil, handler); err != nil {
		t.Fatalf("failed to inject TLS identity: %v", err)
	}
	return out
}

func TestAuthorizer_GroupScopedAccess(t *testing.T) {
	a, rows := newTestAuthorizer(t)
	seedSpecRecord(t, rows, "core/v1/User", "alice.prod", UserSpec{
		Groups: []string{"traders"},
	})
	seedSpecRecord(t, rows, "core/v1/Role", "quota-reader", RoleSpec{
		Tradespaces: []string{"prod"},
		Rules: []RoleRule{{
			Resources: []string{recordResource("core/v1/Quota")},
			Verbs:     []string{VerbGet, VerbList},
		}},
	})
	seedSpecRecord(t, rows, "core/v1/RoleBinding", "quota-reader-binding", RoleBindingSpec{
		RoleRef: "quota-reader",
		Subjects: []Subject{{
			Kind: SubjectGroup,
			Name: "traders",
		}},
	})

	ctx := contextWithCN(t, "alice.prod")

	if err := a.AuthorizeRecordRead(ctx, VerbGet, "core/v1/Quota", "prod"); err != nil {
		t.Fatalf("expected scoped get to succeed, got: %v", err)
	}
	if err := a.AuthorizeRecordRead(ctx, VerbGet, "core/v1/Quota", "staging"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected staging get to be denied, got: %v", err)
	}
	if err := a.AuthorizeRecordRead(ctx, VerbList, "core/v1/Quota", ""); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected unscoped list to be denied, got: %v", err)
	}
}

func TestAuthorizer_GlobalRecordsRequireAllTradespaces(t *testing.T) {
	a, rows := newTestAuthorizer(t)
	seedSpecRecord(t, rows, "core/v1/User", "ops.admin", UserSpec{})
	seedSpecRecord(t, rows, "core/v1/Role", "scoped-user-reader", RoleSpec{
		Tradespaces: []string{"prod"},
		Rules: []RoleRule{{
			Resources: []string{recordResource("core/v1/User")},
			Verbs:     []string{VerbGet},
		}},
	})
	seedSpecRecord(t, rows, "core/v1/RoleBinding", "scoped-user-reader-binding", RoleBindingSpec{
		RoleRef: "scoped-user-reader",
		Subjects: []Subject{{
			Kind: SubjectUser,
			Name: "ops.admin",
		}},
	})

	ctx := contextWithCN(t, "ops.admin")
	if err := a.AuthorizeRecordRead(ctx, VerbGet, "core/v1/User", "default"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected global read without allTradespaces to be denied, got: %v", err)
	}

	seedSpecRecord(t, rows, "core/v1/Role", "global-user-reader", RoleSpec{
		AllTradespaces: true,
		Rules: []RoleRule{{
			Resources: []string{recordResource("core/v1/User")},
			Verbs:     []string{VerbGet},
		}},
	})
	seedSpecRecord(t, rows, "core/v1/RoleBinding", "global-user-reader-binding", RoleBindingSpec{
		RoleRef: "global-user-reader",
		Subjects: []Subject{{
			Kind: SubjectUser,
			Name: "ops.admin",
		}},
	})

	if err := a.AuthorizeRecordRead(ctx, VerbGet, "core/v1/User", "default"); err != nil {
		t.Fatalf("expected global read with allTradespaces to succeed, got: %v", err)
	}
}

func TestAuthorizer_UpdateSpecAndStatusPermissions(t *testing.T) {
	a, rows := newTestAuthorizer(t)
	seedSpecRecord(t, rows, "core/v1/User", "controller.default", UserSpec{})
	seedSpecRecord(t, rows, "core/v1/Role", "allocation-status", RoleSpec{
		Tradespaces: []string{"prod"},
		Rules: []RoleRule{{
			Resources: []string{recordResource("core/v1/Allocation")},
			Verbs:     []string{VerbUpdateStatus},
		}},
	})
	seedSpecRecord(t, rows, "core/v1/RoleBinding", "allocation-status-binding", RoleBindingSpec{
		RoleRef: "allocation-status",
		Subjects: []Subject{{
			Kind: SubjectUser,
			Name: "controller.default",
		}},
	})

	current := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Allocation"},
		ObjectMeta: record.ObjectMeta{
			Name:       "alloc-1",
			Tradespace: "prod",
		},
		Spec: map[string]any{
			"currency":   "USD",
			"amount":     "-10.00",
			"targetType": "polymarket/v1/Order",
			"targetName": "order-1",
		},
	}
	statusOnly := &record.Record{
		TypeMeta:   current.TypeMeta,
		ObjectMeta: current.ObjectMeta,
		Spec:       current.Spec,
		Status: map[string]any{
			"phase": "Approved",
		},
	}
	specOnly := &record.Record{
		TypeMeta:   current.TypeMeta,
		ObjectMeta: current.ObjectMeta,
		Spec: map[string]any{
			"currency":   "USD",
			"amount":     "-20.00",
			"targetType": "polymarket/v1/Order",
			"targetName": "order-1",
		},
	}
	mixed := &record.Record{
		TypeMeta:   current.TypeMeta,
		ObjectMeta: current.ObjectMeta,
		Spec:       specOnly.Spec,
		Status:     statusOnly.Status,
	}

	ctx := contextWithCN(t, "controller.default")
	if err := a.AuthorizeRecordUpdate(ctx, current, statusOnly); err != nil {
		t.Fatalf("expected status-only update to succeed, got: %v", err)
	}
	if err := a.AuthorizeRecordUpdate(ctx, current, specOnly); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected spec-only update to be denied, got: %v", err)
	}
	if err := a.AuthorizeRecordUpdate(ctx, current, mixed); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected mixed update without update_spec to be denied, got: %v", err)
	}

	seedSpecRecord(t, rows, "core/v1/Role", "allocation-spec", RoleSpec{
		Tradespaces: []string{"prod"},
		Rules: []RoleRule{{
			Resources: []string{recordResource("core/v1/Allocation")},
			Verbs:     []string{VerbUpdateSpec},
		}},
	})
	seedSpecRecord(t, rows, "core/v1/RoleBinding", "allocation-spec-binding", RoleBindingSpec{
		RoleRef: "allocation-spec",
		Subjects: []Subject{{
			Kind: SubjectUser,
			Name: "controller.default",
		}},
	})

	if err := a.AuthorizeRecordUpdate(ctx, current, mixed); err != nil {
		t.Fatalf("expected mixed update with both permissions to succeed, got: %v", err)
	}
}

func TestAuthorizer_AllocationCreateBySign(t *testing.T) {
	a, rows := newTestAuthorizer(t)
	seedSpecRecord(t, rows, "core/v1/User", "bob.prod", UserSpec{})
	seedSpecRecord(t, rows, "core/v1/Role", "positive-only", RoleSpec{
		Tradespaces: []string{"prod"},
		Rules: []RoleRule{{
			Resources: []string{recordResource("core/v1/Allocation")},
			Verbs:     []string{VerbCreatePositive},
		}},
	})
	seedSpecRecord(t, rows, "core/v1/RoleBinding", "positive-only-binding", RoleBindingSpec{
		RoleRef: "positive-only",
		Subjects: []Subject{{
			Kind: SubjectUser,
			Name: "bob.prod",
		}},
	})

	ctx := contextWithCN(t, "bob.prod")
	positive := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Allocation"},
		ObjectMeta: record.ObjectMeta{
			Name:       "deposit-1",
			Tradespace: "prod",
		},
		Spec: map[string]any{
			"currency":   "USD",
			"amount":     "100.00",
			"targetType": "core/v1/Deposit",
			"targetName": "deposit-1",
		},
	}
	negative := &record.Record{
		TypeMeta:   positive.TypeMeta,
		ObjectMeta: record.ObjectMeta{Name: "order-1", Tradespace: "prod"},
		Spec: map[string]any{
			"currency":   "USD",
			"amount":     "-5.00",
			"targetType": "polymarket/v1/Order",
			"targetName": "order-1",
		},
	}

	if err := a.AuthorizeRecordCreate(ctx, positive); err != nil {
		t.Fatalf("expected positive create to succeed, got: %v", err)
	}
	if err := a.AuthorizeRecordCreate(ctx, negative); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected negative create to be denied, got: %v", err)
	}
}

func TestAuthorizer_BootstrapBypassOnlyForRBACResources(t *testing.T) {
	a, _ := newTestAuthorizer(t, "bootstrap.default")
	ctx := contextWithCN(t, "bootstrap.default")

	role := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Role"},
		ObjectMeta: record.ObjectMeta{
			Name:       "admin",
			Tradespace: "default",
		},
		Spec: map[string]any{
			"allTradespaces": true,
			"rules":          []any{},
		},
	}
	quota := &record.Record{
		TypeMeta: record.TypeMeta{Group: "core", Version: "v1", Kind: "Quota"},
		ObjectMeta: record.ObjectMeta{
			Name:       "quota",
			Tradespace: "prod",
		},
		Spec: map[string]any{
			"balances": map[string]any{"USD": "100"},
		},
	}

	if err := a.AuthorizeRecordCreate(ctx, role); err != nil {
		t.Fatalf("expected bootstrap role create to succeed, got: %v", err)
	}
	if err := a.AuthorizeRecordCreate(ctx, quota); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("expected bootstrap quota create to be denied, got: %v", err)
	}
}
