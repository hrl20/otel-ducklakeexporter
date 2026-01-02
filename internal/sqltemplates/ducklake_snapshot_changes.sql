CREATE TABLE IF NOT EXISTS %s.ducklake_snapshot_changes (
    snapshot_id BIGINT PRIMARY KEY,
    changes_made VARCHAR,
    author VARCHAR,
    commit_message VARCHAR,
    commit_extra_info VARCHAR
);
