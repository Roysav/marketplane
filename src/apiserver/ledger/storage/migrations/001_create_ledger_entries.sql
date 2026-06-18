CREATE TABLE ledger_entries (
    id             BIGSERIAL PRIMARY KEY,
    from_principal TEXT        NOT NULL,
    to_principal   TEXT        NOT NULL,
    currency       TEXT        NOT NULL,
    amount         NUMERIC     NOT NULL,
    subject        TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
