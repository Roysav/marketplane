CREATE UNIQUE INDEX IF NOT EXISTS idx_ledger_allocation_unique
    ON ledger(tradespace, allocation_name);
