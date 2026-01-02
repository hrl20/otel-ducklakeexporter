CREATE TABLE IF NOT EXISTS %s.ducklake_view (
    view_id BIGINT,
    view_uuid UUID,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    schema_id BIGINT,
    view_name VARCHAR,
    dialect VARCHAR,
    sql VARCHAR,
    column_aliases VARCHAR
);
