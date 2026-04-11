package authz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/roysav/marketplane/pkg/auth"
	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/storage"
	"github.com/roysav/marketplane/pkg/validator"
)

const (
	ResourceAll         = "*"
	ResourceLedgerEntry = "ledger:entry"
	ResourceRecordPrefx = "record:"

	VerbGet            = "get"
	VerbList           = "list"
	VerbWatch          = "watch"
	VerbCreate         = "create"
	VerbCreatePositive = "create_positive"
	VerbCreateNegative = "create_negative"
	VerbUpdateSpec     = "update_spec"
	VerbUpdateStatus   = "update_status"
	VerbDelete         = "delete"
	VerbAppend         = "append"

	SubjectUser  = "User"
	SubjectGroup = "Group"
)

var (
	ErrUnauthenticated     = errors.New("unauthenticated")
	ErrPermissionDenied    = errors.New("permission denied")
	ErrInvalidRecordUpdate = errors.New("invalid record update")
)

var bootstrapResources = map[string]struct{}{
	recordResource("core/v1/User"):        {},
	recordResource("core/v1/Role"):        {},
	recordResource("core/v1/RoleBinding"): {},
}

type Config struct {
	Rows              storage.RowStorage
	Enabled           bool
	BootstrapAdminCNs []string
}

type Authorizer struct {
	rows      storage.RowStorage
	validator *validator.Validator
	enabled   bool
	bootstrap map[string]struct{}
}

type UserSpec struct {
	Description string   `json:"description,omitempty"`
	Groups      []string `json:"groups,omitempty"`
}

type RoleRule struct {
	Resources []string `json:"resources"`
	Verbs     []string `json:"verbs"`
}

type RoleSpec struct {
	AllTradespaces bool       `json:"allTradespaces,omitempty"`
	Tradespaces    []string   `json:"tradespaces,omitempty"`
	Rules          []RoleRule `json:"rules"`
}

type Subject struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

type RoleBindingSpec struct {
	RoleRef  string    `json:"roleRef"`
	Subjects []Subject `json:"subjects"`
}

type principal struct {
	name   string
	groups map[string]struct{}
}

func New(cfg Config) *Authorizer {
	a := &Authorizer{
		rows:      cfg.Rows,
		validator: validator.New(cfg.Rows),
		enabled:   cfg.Enabled,
		bootstrap: make(map[string]struct{}, len(cfg.BootstrapAdminCNs)),
	}
	for _, cn := range cfg.BootstrapAdminCNs {
		if cn == "" {
			continue
		}
		a.bootstrap[cn] = struct{}{}
	}
	return a
}

func (a *Authorizer) Enabled() bool {
	return a != nil && a.enabled
}

func (a *Authorizer) AuthorizeRecordRead(ctx context.Context, verb, typeStr, tradespace string) error {
	if !a.Enabled() {
		return nil
	}
	return a.authorizeAll(ctx, recordResource(typeStr), tradespace, a.isGlobalRecordType(ctx, typeStr), verb)
}

func (a *Authorizer) AuthorizeRecordCreate(ctx context.Context, r *record.Record) error {
	if !a.Enabled() {
		return nil
	}

	typeStr := r.TypeMeta.GVK().Type()
	resource := recordResource(typeStr)
	if typeStr == "core/v1/Allocation" {
		amount, err := allocationAmount(r.Spec)
		if err != nil {
			return err
		}
		if amount.IsPositive() {
			return a.authorizeAny(ctx, resource, r.ObjectMeta.Tradespace, false, VerbCreate, VerbCreatePositive)
		}
		return a.authorizeAny(ctx, resource, r.ObjectMeta.Tradespace, false, VerbCreate, VerbCreateNegative)
	}

	return a.authorizeAll(ctx, resource, r.ObjectMeta.Tradespace, a.isGlobalRecordType(ctx, typeStr), VerbCreate)
}

func (a *Authorizer) AuthorizeRecordUpdate(ctx context.Context, current, updated *record.Record) error {
	if !a.Enabled() {
		return nil
	}
	verbs, err := determineUpdateVerbs(current, updated)
	if err != nil {
		return err
	}

	typeStr := current.TypeMeta.GVK().Type()
	return a.authorizeAll(ctx, recordResource(typeStr), current.ObjectMeta.Tradespace, a.isGlobalRecordType(ctx, typeStr), verbs...)
}

func (a *Authorizer) AuthorizeRecordDelete(ctx context.Context, typeStr, tradespace string) error {
	if !a.Enabled() {
		return nil
	}
	return a.authorizeAll(ctx, recordResource(typeStr), tradespace, a.isGlobalRecordType(ctx, typeStr), VerbDelete)
}

func (a *Authorizer) AuthorizeLedger(ctx context.Context, verb, tradespace string) error {
	if !a.Enabled() {
		return nil
	}
	return a.authorizeAll(ctx, ResourceLedgerEntry, tradespace, false, verb)
}

func (a *Authorizer) authorizeAny(ctx context.Context, resource, tradespace string, isGlobal bool, verbs ...string) error {
	allowed, err := a.allowedVerbs(ctx, resource, tradespace, isGlobal)
	if err != nil {
		return err
	}
	for _, verb := range verbs {
		if allowed[verb] || allowed[ResourceAll] {
			return nil
		}
	}
	return fmt.Errorf("%w: resource=%s verbs=%v", ErrPermissionDenied, resource, verbs)
}

func (a *Authorizer) authorizeAll(ctx context.Context, resource, tradespace string, isGlobal bool, verbs ...string) error {
	allowed, err := a.allowedVerbs(ctx, resource, tradespace, isGlobal)
	if err != nil {
		return err
	}
	for _, verb := range verbs {
		if !allowed[verb] && !allowed[ResourceAll] {
			return fmt.Errorf("%w: resource=%s verb=%s", ErrPermissionDenied, resource, verb)
		}
	}
	return nil
}

func (a *Authorizer) allowedVerbs(ctx context.Context, resource, tradespace string, isGlobal bool) (map[string]bool, error) {
	principal, bypass, err := a.resolvePrincipal(ctx, resource)
	if err != nil {
		return nil, err
	}
	if bypass {
		return map[string]bool{ResourceAll: true}, nil
	}

	bindings, err := a.listBindings(ctx)
	if err != nil {
		return nil, err
	}

	roleNames := make(map[string]struct{})
	for _, binding := range bindings {
		if binding.RoleRef == "" {
			continue
		}
		if bindingMatches(binding.Subjects, principal) {
			roleNames[binding.RoleRef] = struct{}{}
		}
	}

	allowed := make(map[string]bool)
	for roleName := range roleNames {
		role, err := a.getRole(ctx, roleName)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return nil, err
		}
		if !scopeAllows(role, tradespace, isGlobal) {
			continue
		}
		for _, rule := range role.Rules {
			if !matchesResource(rule.Resources, resource) {
				continue
			}
			for _, verb := range rule.Verbs {
				allowed[verb] = true
			}
		}
	}

	return allowed, nil
}

func (a *Authorizer) resolvePrincipal(ctx context.Context, resource string) (*principal, bool, error) {
	id, ok := auth.FromContext(ctx)
	if !ok || id == nil || id.CommonName == "" {
		return nil, false, ErrUnauthenticated
	}
	if _, ok := a.bootstrap[id.CommonName]; ok && bootstrapAllowed(resource) {
		return &principal{name: id.CommonName, groups: map[string]struct{}{}}, true, nil
	}

	user, err := a.getUser(ctx, id.CommonName)
	if err != nil {
		if isNotFound(err) {
			return nil, false, fmt.Errorf("%w: unknown user %q", ErrPermissionDenied, id.CommonName)
		}
		return nil, false, err
	}

	groups := make(map[string]struct{}, len(user.Groups))
	for _, group := range user.Groups {
		groups[group] = struct{}{}
	}

	return &principal{name: id.CommonName, groups: groups}, false, nil
}

func (a *Authorizer) getUser(ctx context.Context, name string) (*UserSpec, error) {
	row, err := a.rows.Get(ctx, storage.Key{
		Type:       "core/v1/User",
		Tradespace: "default",
		Name:       name,
	})
	if err != nil {
		return nil, err
	}
	spec := &UserSpec{}
	if err := decodeSpec(row.Data, spec); err != nil {
		return nil, err
	}
	return spec, nil
}

func (a *Authorizer) getRole(ctx context.Context, name string) (*RoleSpec, error) {
	row, err := a.rows.Get(ctx, storage.Key{
		Type:       "core/v1/Role",
		Tradespace: "default",
		Name:       name,
	})
	if err != nil {
		return nil, err
	}
	spec := &RoleSpec{}
	if err := decodeSpec(row.Data, spec); err != nil {
		return nil, err
	}
	return spec, nil
}

func (a *Authorizer) listBindings(ctx context.Context) ([]RoleBindingSpec, error) {
	rows, err := a.rows.List(ctx, storage.Query{
		Type:       "core/v1/RoleBinding",
		Tradespace: "default",
	})
	if err != nil {
		return nil, err
	}

	bindings := make([]RoleBindingSpec, 0, len(rows))
	for _, row := range rows {
		var binding RoleBindingSpec
		if err := decodeSpec(row.Data, &binding); err != nil {
			return nil, err
		}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func (a *Authorizer) isGlobalRecordType(ctx context.Context, typeStr string) bool {
	scope, err := a.validator.GetScope(ctx, typeStr)
	if err != nil {
		return false
	}
	return scope == validator.ScopeGlobal
}

func bindingMatches(subjects []Subject, principal *principal) bool {
	for _, subject := range subjects {
		switch subject.Kind {
		case SubjectUser:
			if subject.Name == principal.name {
				return true
			}
		case SubjectGroup:
			if _, ok := principal.groups[subject.Name]; ok {
				return true
			}
		}
	}
	return false
}

func matchesResource(resources []string, resource string) bool {
	for _, candidate := range resources {
		if candidate == ResourceAll || candidate == resource {
			return true
		}
	}
	return false
}

func scopeAllows(role *RoleSpec, tradespace string, isGlobal bool) bool {
	if role.AllTradespaces {
		return true
	}
	if isGlobal || tradespace == "" {
		return false
	}
	return slices.Contains(role.Tradespaces, tradespace)
}

func determineUpdateVerbs(current, updated *record.Record) ([]string, error) {
	if current.TypeMeta.GVK().Type() != updated.TypeMeta.GVK().Type() {
		return nil, ErrInvalidRecordUpdate
	}
	if current.ObjectMeta.Name != updated.ObjectMeta.Name || current.ObjectMeta.Tradespace != updated.ObjectMeta.Tradespace {
		return nil, ErrInvalidRecordUpdate
	}

	required := make(map[string]struct{})
	if !reflect.DeepEqual(current.Spec, updated.Spec) ||
		!maps.Equal(current.ObjectMeta.Labels, updated.ObjectMeta.Labels) ||
		!maps.Equal(current.ObjectMeta.Annotations, updated.ObjectMeta.Annotations) {
		required[VerbUpdateSpec] = struct{}{}
	}
	if !reflect.DeepEqual(current.Status, updated.Status) {
		required[VerbUpdateStatus] = struct{}{}
	}
	if len(required) == 0 {
		return []string{VerbUpdateSpec}, nil
	}

	verbs := make([]string, 0, len(required))
	for verb := range required {
		verbs = append(verbs, verb)
	}
	slices.Sort(verbs)
	return verbs, nil
}

func allocationAmount(spec map[string]any) (decimal.Decimal, error) {
	amountStr, _ := spec["amount"].(string)
	amount, err := decimal.NewFromString(amountStr)
	if err != nil {
		return decimal.Zero, fmt.Errorf("%w: invalid allocation amount", ErrInvalidRecordUpdate)
	}
	if amount.IsZero() {
		return decimal.Zero, fmt.Errorf("%w: allocation amount must be non-zero", ErrInvalidRecordUpdate)
	}
	return amount, nil
}

func bootstrapAllowed(resource string) bool {
	_, ok := bootstrapResources[resource]
	return ok
}

func recordResource(typeStr string) string {
	return ResourceRecordPrefx + typeStr
}

func decodeSpec(data string, target any) error {
	var payload struct {
		Spec json.RawMessage `json:"spec"`
	}
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return fmt.Errorf("decode record payload: %w", err)
	}
	if len(payload.Spec) == 0 || string(payload.Spec) == "null" {
		return nil
	}
	if err := json.Unmarshal(payload.Spec, target); err != nil {
		return fmt.Errorf("decode record spec: %w", err)
	}
	return nil
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, storage.ErrNotFound) || strings.Contains(strings.ToLower(err.Error()), "not found")
}
