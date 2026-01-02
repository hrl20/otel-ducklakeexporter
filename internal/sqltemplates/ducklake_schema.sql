CREATE TABLE IF NOT EXISTS %s.ducklake_schema (
    schema_id BIGINT PRIMARY KEY,
    schema_uuid UUID,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    schema_name VARCHAR,
    path VARCHAR,
    path_is_relative BOOLEAN
);
