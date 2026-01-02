CREATE TABLE IF NOT EXISTS %s.ducklake_partition_column (
    partition_id BIGINT,
    table_id BIGINT,
    partition_key_index BIGINT,
    column_id BIGINT,
    transform VARCHAR
);
