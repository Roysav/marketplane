-- Records table for storing all record types
CREATE TABLE IF NOT EXISTS records (
    type TEXT NOT NULL,
    tradespace TEXT NOT NULL,
    name TEXT NOT NULL,
    labels JSONB DEFAULT '{}',
    data JSONB DEFAULT '{}',
    resource_version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (type, tradespace, name)
);

CREATE INDEX IF NOT EXISTS idx_records_tradespace ON records(type, tradespace);
CREATE INDEX IF NOT EXISTS idx_records_labels ON records USING GIN(labels);
