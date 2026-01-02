CREATE TABLE IF NOT EXISTS %s.ducklake_delete_file (
    delete_file_id BIGINT PRIMARY KEY,
    table_id BIGINT,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    data_file_id BIGINT,
    path VARCHAR,
    path_is_relative BOOLEAN,
    format VARCHAR,
    delete_count BIGINT,
    file_size_bytes BIGINT,
    footer_size BIGINT,
    encryption_key VARCHAR
);
