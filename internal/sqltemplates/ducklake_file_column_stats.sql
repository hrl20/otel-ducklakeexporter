CREATE TABLE IF NOT EXISTS %s.ducklake_file_column_stats (
    data_file_id BIGINT,
    table_id BIGINT,
    column_id BIGINT,
    column_size_bytes BIGINT,
    value_count BIGINT,
    null_count BIGINT,
    min_value VARCHAR,
    max_value VARCHAR,
    contains_nan BOOLEAN,
    extra_stats VARCHAR
);
