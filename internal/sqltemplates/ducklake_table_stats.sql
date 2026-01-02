CREATE TABLE IF NOT EXISTS %s.ducklake_table_stats (
    table_id BIGINT,
    record_count BIGINT,
    next_row_id BIGINT,
    file_size_bytes BIGINT
);
