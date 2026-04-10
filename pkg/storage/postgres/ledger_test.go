package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/roysav/marketplane/pkg/storage"
)

func newTestLedgerStorage(t *testing.T) *LedgerStorage {
	t.Helper()
	ctx := context.Background()

	s, err := NewLedgerStorage(ctx, testDSN)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}

	// Clean up tables for test isolation
	s.db.ExecContext(ctx, "TRUNCATE ledger CASCADE")

	t.Cleanup(func() { s.Close() })
	return s
}

func TestLedgerAppend_Deposit(t *testing.T) {
	s := newTestLedgerStorage(t)
	ctx := context.Background()

	entry := &storage.LedgerEntry{
		Tradespace:     "trading-ns",
		Currency:       "USD",
		Amount:         "1000.00",
		AllocationName: "deposit-001",
		TargetType:     "core/v1/Deposit",
		TargetName:     "deposit-001",
	}

	err := s.Append(ctx, entry)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify ID was generated
	if entry.ID == "" {
		t.Error("ID should be generated")
	}

	// Verify balance
	balance, err := s.Balance(ctx, "trading-ns", "USD")
	if err != nil {
		t.Fatalf("Balance failed: %v", err)
	}
	if balance != "1000.00" && balance != "1000" {
		t.Errorf("Balance = %s, want 1000.00", balance)
	}
}

func TestLedgerAppend_Spend(t *testing.T) {
	s := newTestLedgerStorage(t)
	ctx := context.Background()

	// First deposit
	err := s.Append(ctx, &storage.LedgerEntry{
		Tradespace:     "trading-ns",
		Currency:       "USD",
		Amount:         "1000.00",
		AllocationName: "deposit-001",
		TargetType:     "core/v1/Deposit",
		TargetName:     "deposit-001",
	})
	if err != nil {
		t.Fatalf("Append deposit failed: %v", err)
	}

	// Then spend
	err = s.Append(ctx, &storage.LedgerEntry{
		Tradespace:     "trading-ns",
		Currency:       "USD",
		Amount:         "-50.00",
		AllocationName: "order-001",
		TargetType:     "polymarket/v1/Order",
		TargetName:     "order-001",
	})
	if err != nil {
		t.Fatalf("Append spend failed: %v", err)
	}

	// Verify balance
	balance, err := s.Balance(ctx, "trading-ns", "USD")
	if err != nil {
		t.Fatalf("Balance failed: %v", err)
	}
	if balance != "950.00" && balance != "950" {
		t.Errorf("Balance = %s, want 950", balance)
	}
}

func TestLedgerAppend_InsufficientBalance(t *testing.T) {
	s := newTestLedgerStorage(t)
	ctx := context.Background()

	// Deposit 100
	err := s.Append(ctx, &storage.LedgerEntry{
		Tradespace:     "trading-ns",
		Currency:       "USD",
		Amount:         "100.00",
		AllocationName: "deposit-001",
		TargetType:     "core/v1/Deposit",
		TargetName:     "deposit-001",
	})
	if err != nil {
		t.Fatalf("Append deposit failed: %v", err)
	}

	// Try to spend 150 (should fail)
	err = s.Append(ctx, &storage.LedgerEntry{
		Tradespace:     "trading-ns",
		Currency:       "USD",
		Amount:         "-150.00",
		AllocationName: "order-001",
		TargetType:     "polymarket/v1/Order",
		TargetName:     "order-001",
	})
	if !errors.Is(err, storage.ErrInsufficientBalance) {
		t.Errorf("expected ErrInsufficientBalance, got: %v", err)
	}
}

func TestLedgerAppend_AlreadyAllocated(t *testing.T) {
	s := newTestLedgerStorage(t)
	ctx := context.Background()

	entry := &storage.LedgerEntry{
		Tradespace:     "trading-ns",
		Currency:       "USD",
		Amount:         "100.00",
		AllocationName: "deposit-001",
		TargetType:     "core/v1/Deposit",
		TargetName:     "deposit-001",
	}

	err := s.Append(ctx, entry)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Second append for same target should fail
	entry2 := &storage.LedgerEntry{
		Tradespace:     "trading-ns",
		Currency:       "USD",
		Amount:         "200.00",
		AllocationName: "deposit-002",
		TargetType:     "core/v1/Deposit",
		TargetName:     "deposit-001", // Same target
	}
	err = s.Append(ctx, entry2)
	if !errors.Is(err, storage.ErrAlreadyAllocated) {
		t.Errorf("expected ErrAlreadyAllocated, got: %v", err)
	}
}

func TestLedgerAppend_Concurrent(t *testing.T) {
	s := newTestLedgerStorage(t)
	ctx := context.Background()

	// Initial deposit
	err := s.Append(ctx, &storage.LedgerEntry{
		Tradespace:     "trading-ns",
		Currency:       "USD",
		Amount:         "100.00",
		AllocationName: "deposit-001",
		TargetType:     "core/v1/Deposit",
		TargetName:     "deposit-001",
	})
	if err != nil {
		t.Fatalf("Append deposit failed: %v", err)
	}

	// Try to spend concurrently - each goroutine tries to spend 60
	// Only one should succeed since balance is 100
	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := s.Append(ctx, &storage.LedgerEntry{
				Tradespace:     "trading-ns",
				Currency:       "USD",
				Amount:         "-60.00",
				AllocationName: "order-" + string(rune('a'+idx)),
				TargetType:     "polymarket/v1/Order",
				TargetName:     "order-" + string(rune('a'+idx)),
			})
			if err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Only one should have succeeded
	if successCount != 1 {
		t.Errorf("Expected exactly 1 successful spend, got %d", successCount)
	}

	// Final balance should be 40
	balance, _ := s.Balance(ctx, "trading-ns", "USD")
	if balance != "40.00" && balance != "40" {
		t.Errorf("Balance = %s, want 40", balance)
	}
}

func TestLedgerGetByTarget(t *testing.T) {
	s := newTestLedgerStorage(t)
	ctx := context.Background()

	entry := &storage.LedgerEntry{
		Tradespace:     "trading-ns",
		Currency:       "USD",
		Amount:         "100.00",
		AllocationName: "deposit-001",
		TargetType:     "core/v1/Deposit",
		TargetName:     "deposit-001",
	}

	err := s.Append(ctx, entry)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	got, err := s.GetByTarget(ctx, "trading-ns", "core/v1/Deposit", "deposit-001")
	if err != nil {
		t.Fatalf("GetByTarget failed: %v", err)
	}

	if got.Amount != "100.00" && got.Amount != "100" {
		t.Errorf("Amount = %s, want 100.00", got.Amount)
	}
	if got.Currency != "USD" {
		t.Errorf("Currency = %s, want USD", got.Currency)
	}
}

func TestLedgerGetByTarget_NotFound(t *testing.T) {
	s := newTestLedgerStorage(t)
	ctx := context.Background()

	_, err := s.GetByTarget(ctx, "trading-ns", "core/v1/Deposit", "nonexistent")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestLedgerList(t *testing.T) {
	s := newTestLedgerStorage(t)
	ctx := context.Background()

	// Add multiple entries
	for i := 0; i < 3; i++ {
		name := string(rune('a' + i))
		err := s.Append(ctx, &storage.LedgerEntry{
			Tradespace:     "trading-ns",
			Currency:       "USD",
			Amount:         "100.00",
			AllocationName: "deposit-" + name,
			TargetType:     "core/v1/Deposit",
			TargetName:     "deposit-" + name,
		})
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	entries, err := s.List(ctx, "trading-ns")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("List returned %d entries, want 3", len(entries))
	}
}

func TestLedgerAppend_MultipleCurrencies(t *testing.T) {
	s := newTestLedgerStorage(t)
	ctx := context.Background()

	// Deposit USD
	err := s.Append(ctx, &storage.LedgerEntry{
		Tradespace:     "trading-ns",
		Currency:       "USD",
		Amount:         "1000.00",
		AllocationName: "deposit-usd",
		TargetType:     "core/v1/Deposit",
		TargetName:     "deposit-usd",
	})
	if err != nil {
		t.Fatalf("Append USD failed: %v", err)
	}

	// Deposit EUR
	err = s.Append(ctx, &storage.LedgerEntry{
		Tradespace:     "trading-ns",
		Currency:       "EUR",
		Amount:         "500.00",
		AllocationName: "deposit-eur",
		TargetType:     "core/v1/Deposit",
		TargetName:     "deposit-eur",
	})
	if err != nil {
		t.Fatalf("Append EUR failed: %v", err)
	}

	// Verify balances are separate
	usdBalance, _ := s.Balance(ctx, "trading-ns", "USD")
	eurBalance, _ := s.Balance(ctx, "trading-ns", "EUR")

	if usdBalance != "1000.00" && usdBalance != "1000" {
		t.Errorf("USD Balance = %s, want 1000", usdBalance)
	}
	if eurBalance != "500.00" && eurBalance != "500" {
		t.Errorf("EUR Balance = %s, want 500", eurBalance)
	}
}
