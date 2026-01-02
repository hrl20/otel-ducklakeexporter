CREATE TABLE IF NOT EXISTS %s.ducklake_table_column_stats (
    table_id BIGINT,
    column_id BIGINT,
    contains_null BOOLEAN,
    contains_nan BOOLEAN,
    min_value VARCHAR,
    max_value VARCHAR,
    extra_stats VARCHAR
);
