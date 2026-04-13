package service_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/service"
	"github.com/roysav/marketplane/pkg/storage"
	"github.com/roysav/marketplane/tests"
)

func newLedgerTestService(t *testing.T) (*service.LedgerService, *service.RecordService) {
	t.Helper()
	ctx := context.Background()
	return tests.LedgerSVC(ctx, t)
}

func createAllocation(t *testing.T, svc *service.RecordService, tradespace, name, amount, currency string) *record.Record {
	t.Helper()
	ctx := context.Background()

	r := &record.Record{
		Type: "core/v1/Allocation",
		ObjectMeta: record.ObjectMeta{
			Tradespace: tradespace,
			Name:       name,
		},
		Spec: map[string]any{
			"currency":   currency,
			"amount":     amount,
			"targetType": "polymarket/v1/Order",
			"targetName": "order-1",
		},
	}

	created, err := svc.Create(ctx, r)
	require.NoError(t, err)
	return created
}

func TestLedger_AppendSuccess(t *testing.T) {
	ledgerSvc, recordSvc := newLedgerTestService(t)
	ctx := context.Background()

	createAllocation(t, recordSvc, "prod", "deposit-1", "100", "USD")

	err := ledgerSvc.Append(ctx, "deposit-1", "prod", "100", "USD")
	require.NoError(t, err)
}

func TestLedger_AppendAllocationNotFound(t *testing.T) {
	ledgerSvc, _ := newLedgerTestService(t)
	ctx := context.Background()

	err := ledgerSvc.Append(ctx, "nonexistent", "prod", "100", "USD")
	assert.ErrorIs(t, err, service.ErrNotFound)
}

func TestLedger_AppendCurrencyMismatch(t *testing.T) {
	ledgerSvc, recordSvc := newLedgerTestService(t)
	ctx := context.Background()

	createAllocation(t, recordSvc, "prod", "deposit-2", "100", "USD")

	err := ledgerSvc.Append(ctx, "deposit-2", "prod", "100", "EUR")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected currency")
}

func TestLedger_AppendAmountMismatch(t *testing.T) {
	ledgerSvc, recordSvc := newLedgerTestService(t)
	ctx := context.Background()

	createAllocation(t, recordSvc, "prod", "deposit-3", "100", "USD")

	err := ledgerSvc.Append(ctx, "deposit-3", "prod", "999", "USD")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected amount")
}

func TestLedger_AppendInsufficientBalance(t *testing.T) {
	ledgerSvc, recordSvc := newLedgerTestService(t)
	ctx := context.Background()

	createAllocation(t, recordSvc, "prod", "spend-1", "-500", "USD")

	err := ledgerSvc.Append(ctx, "spend-1", "prod", "-500", "USD")
	assert.ErrorIs(t, err, storage.ErrInsufficientBalance)
}

func TestLedger_AppendDuplicate(t *testing.T) {
	ledgerSvc, recordSvc := newLedgerTestService(t)
	ctx := context.Background()

	createAllocation(t, recordSvc, "prod", "deposit-dup", "50", "USD")

	err := ledgerSvc.Append(ctx, "deposit-dup", "prod", "50", "USD")
	require.NoError(t, err)

	err = ledgerSvc.Append(ctx, "deposit-dup", "prod", "50", "USD")
	assert.Error(t, err)
}

func TestLedger_AppendAndSpend(t *testing.T) {
	ledgerSvc, recordSvc := newLedgerTestService(t)
	ctx := context.Background()

	// Deposit first
	createAllocation(t, recordSvc, "prod", "fund-1", "200", "USD")
	require.NoError(t, ledgerSvc.Append(ctx, "fund-1", "prod", "200", "USD"))

	// Spend within balance
	createAllocation(t, recordSvc, "prod", "spend-2", "-100", "USD")
	require.NoError(t, ledgerSvc.Append(ctx, "spend-2", "prod", "-100", "USD"))

	// Verify via List
	entries, err := ledgerSvc.List(ctx, "prod")
	require.NoError(t, err)
	assert.Len(t, entries, 2)
}

func TestLedger_ListEmpty(t *testing.T) {
	ledgerSvc, _ := newLedgerTestService(t)
	ctx := context.Background()

	entries, err := ledgerSvc.List(ctx, "empty-tradespace")
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestLedger_ListIsolation(t *testing.T) {
	ledgerSvc, recordSvc := newLedgerTestService(t)
	ctx := context.Background()

	// Create allocations in two different tradespaces
	createAllocation(t, recordSvc, "ts-a", "alloc-1", "100", "USD")
	createAllocation(t, recordSvc, "ts-b", "alloc-2", "200", "EUR")

	require.NoError(t, ledgerSvc.Append(ctx, "alloc-1", "ts-a", "100", "USD"))
	require.NoError(t, ledgerSvc.Append(ctx, "alloc-2", "ts-b", "200", "EUR"))

	entriesA, err := ledgerSvc.List(ctx, "ts-a")
	require.NoError(t, err)
	assert.Len(t, entriesA, 1)
	assert.Equal(t, "100", entriesA[0].Amount)

	entriesB, err := ledgerSvc.List(ctx, "ts-b")
	require.NoError(t, err)
	assert.Len(t, entriesB, 1)
	assert.Equal(t, "200", entriesB[0].Amount)
}