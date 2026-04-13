package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/storage"
)

type LedgerService struct {
	storage       storage.LedgerStorage
	recordService RecordService
	logger        *slog.Logger
}

func NewLedgerService(storage storage.LedgerStorage, recordService RecordService, logger *slog.Logger) *LedgerService {
	return &LedgerService{storage: storage, recordService: recordService, logger: logger}
}

func (lsvc *LedgerService) Append(ctx context.Context, allocationName, tradespace, amount, currency string) error {
	// Validate an allocation actually exists
	allocation, err := lsvc.recordService.Get(ctx, "core/v1/Allocation", tradespace, allocationName)
	if err != nil {
		return err
	}

	alloCurrency := allocation.Spec["currency"].(string)
	alloAmount := allocation.Spec["amount"].(string)

	if currency != alloCurrency {
		return fmt.Errorf("failed to append ledger: expected currency %s got %s", alloCurrency, amount)
	}
	if alloAmount != amount {
		return fmt.Errorf("failed to append ledger: expected amount of %s got %s", alloCurrency, currency)
	}

	key := record.Key("core/v1/Allocation", tradespace, allocationName)
	prefix := record.Key("core/v1/Allocation", tradespace, "")
	return lsvc.storage.Append(ctx, prefix, key, amount, currency)
}
