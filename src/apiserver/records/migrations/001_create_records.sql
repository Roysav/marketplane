CREATE TABLE records (
    key     TEXT    PRIMARY KEY,
    value   BYTEA   NOT NULL,
    indexes TEXT[]  NOT NULL
);

CREATE INDEX records_indexes_gin ON records USING GIN (indexes);
