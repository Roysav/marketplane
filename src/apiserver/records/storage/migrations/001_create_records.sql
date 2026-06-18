CREATE TABLE records (
    key      TEXT    PRIMARY KEY,
    value    BYTEA   NOT NULL,
    indexes  TEXT[]  NOT NULL,
    revision BIGINT  NOT NULL DEFAULT 0
);

CREATE INDEX records_indexes_gin ON records USING GIN (indexes);
