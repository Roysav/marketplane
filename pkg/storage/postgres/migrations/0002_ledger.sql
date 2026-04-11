-- Ledger table for tracking allocations
-- Uses NUMERIC for precise decimal amounts
-- Advisory locks are used at runtime for serialization
CREATE TABLE IF NOT EXISTS ledger (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tradespace TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    allocation_name TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- One-to-one: each target can only have one allocation
    UNIQUE(tradespace, target_type, target_name),

    -- Each Allocation can only be approved once
    UNIQUE(tradespace, allocation_name)
);

-- Index for balance queries (SUM by tradespace+currency)
CREATE INDEX IF NOT EXISTS idx_ledger_balance ON ledger(tradespace, currency);

-- Index for listing by tradespace
CREATE INDEX IF NOT EXISTS idx_ledger_tradespace ON ledger(tradespace);
