ALTER TABLE records
    ADD COLUMN IF NOT EXISTS finalizers JSONB NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS deletion_timestamp TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_records_terminating
    ON records(type, tradespace)
    WHERE deletion_timestamp IS NOT NULL;
