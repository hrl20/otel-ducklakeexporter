CREATE TABLE IF NOT EXISTS %s.ducklake_name_mapping (
    mapping_id BIGINT,
    column_id BIGINT,
    source_name VARCHAR,
    target_field_id BIGINT,
    parent_column BIGINT,
    is_partition BOOLEAN
);
