CREATE TABLE "records" (
    key TEXT PRIMARY KEY,
    data jsonb NOT NULL,
    labels VARCHAR(255)[] NOT NULL
);

CREATE INDEX idx_records_key ON records USING btree (key);
CREATE INDEX idx_records_labels ON records (labels);


CREATE TABLE "ledger" (
    key      TEXT PRIMARY KEY,  --- referencing the record
    amount   DECIMAL,
    currency TEXT
);

CREATE INDEX idx_ledger_key_currency ON ledger (key, currency)
