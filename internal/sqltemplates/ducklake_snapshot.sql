CREATE TABLE IF NOT EXISTS %s.ducklake_snapshot (
    snapshot_id BIGINT PRIMARY KEY,
    snapshot_time TIMESTAMPTZ,
    schema_version BIGINT,
    next_catalog_id BIGINT,
    next_file_id BIGINT
);
