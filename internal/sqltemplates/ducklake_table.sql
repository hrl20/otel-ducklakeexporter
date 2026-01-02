CREATE TABLE IF NOT EXISTS %s.ducklake_table (
    table_id BIGINT,
    table_uuid UUID,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    schema_id BIGINT,
    table_name VARCHAR,
    path VARCHAR,
    path_is_relative BOOLEAN
);
