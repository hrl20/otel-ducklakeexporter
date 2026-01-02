CREATE TABLE IF NOT EXISTS %s.ducklake_files_scheduled_for_deletion (
    data_file_id BIGINT,
    path VARCHAR,
    path_is_relative BOOLEAN,
    schedule_start TIMESTAMPTZ
);
