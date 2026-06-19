CREATE TABLE records (
    key      TEXT    PRIMARY KEY,
    value    BYTEA   NOT NULL,
    indices  TEXT[]  NOT NULL,
    revision BIGINT  NOT NULL
);

CREATE INDEX records_indices_gin ON records USING GIN (indices);
