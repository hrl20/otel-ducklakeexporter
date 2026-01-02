CREATE TABLE IF NOT EXISTS %s.ducklake_partition_info (
    partition_id BIGINT,
    table_id BIGINT,
    begin_snapshot BIGINT,
    end_snapshot BIGINT
);
