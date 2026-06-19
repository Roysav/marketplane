CREATE TABLE ledger_entries (
    id             BIGSERIAL PRIMARY KEY,
    from_principal TEXT      NOT NULL,
    to_principal   TEXT      NOT NULL,
    currency       TEXT      NOT NULL,
    amount         NUMERIC   NOT NULL,
    subject        TEXT      NOT NULL
);

CREATE INDEX ledger_entries_from_idx ON ledger_entries (from_principal, currency);
CREATE INDEX ledger_entries_to_idx ON ledger_entries (to_principal, currency);
