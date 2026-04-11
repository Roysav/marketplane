DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_class t
        JOIN pg_namespace n
            ON n.oid = t.relnamespace
        JOIN pg_index i
            ON i.indrelid = t.oid
        JOIN pg_class idx
            ON idx.oid = i.indexrelid
        JOIN pg_attribute a1
            ON a1.attrelid = t.oid
            AND a1.attnum = i.indkey[0]
        JOIN pg_attribute a2
            ON a2.attrelid = t.oid
            AND a2.attnum = i.indkey[1]
        WHERE t.relname = 'ledger'
          AND n.nspname = current_schema()
          AND i.indisunique
          AND i.indnatts = 2
          AND a1.attname = 'tradespace'
          AND a2.attname = 'allocation_name'
    ) THEN
        CREATE UNIQUE INDEX idx_ledger_allocation_unique
            ON ledger(tradespace, allocation_name);
    END IF;
END $$;
