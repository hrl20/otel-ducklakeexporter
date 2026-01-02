CREATE TABLE IF NOT EXISTS %s.ducklake_column (
    column_id BIGINT,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    table_id BIGINT,
    column_order BIGINT,
    column_name VARCHAR,
    column_type VARCHAR,
    initial_default VARCHAR,
    default_value VARCHAR,
    nulls_allowed BOOLEAN,
    parent_column BIGINT
);
