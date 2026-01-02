CREATE TABLE IF NOT EXISTS %s.ducklake_tag (
    object_id BIGINT,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    key VARCHAR,
    value VARCHAR
);
