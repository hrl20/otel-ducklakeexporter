CREATE TABLE IF NOT EXISTS %s.ducklake_column_tag (
    table_id BIGINT,
    column_id BIGINT,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    key VARCHAR,
    value VARCHAR
);
