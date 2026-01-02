CREATE TABLE IF NOT EXISTS %s.ducklake_file_partition_value (
    data_file_id BIGINT,
    table_id BIGINT,
    partition_key_index BIGINT,
    partition_value VARCHAR
);
