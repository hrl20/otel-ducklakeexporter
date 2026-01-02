CREATE TABLE IF NOT EXISTS %s.ducklake_data_file (
    data_file_id BIGINT PRIMARY KEY,
    table_id BIGINT,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    file_order BIGINT,
    path VARCHAR,
    path_is_relative BOOLEAN,
    file_format VARCHAR,
    record_count BIGINT,
    file_size_bytes BIGINT,
    footer_size BIGINT,
    row_id_start BIGINT,
    partition_id BIGINT,
    encryption_key VARCHAR,
    partial_file_info VARCHAR,
    mapping_id BIGINT
);
