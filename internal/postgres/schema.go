// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package postgres // import "github.com/hrl20/otel-ducklakeexporter/internal/postgres"

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/hrl20/otel-ducklakeexporter/internal/sqltemplates"
)

// CreateSchema creates the DuckLake metadata schema and tables in PostgreSQL.
func (c *Client) CreateSchema(ctx context.Context, dataPath string) error {
	c.logger.Info("creating ducklake metadata schema", zap.String("schema", c.schemaName))

	// Create schema if it doesn't exist
	_, err := c.db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", c.schemaName))
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// Create sequences for ID fields (atomic counters, no race conditions)
	// Note: data_file_id is managed through snapshot.next_file_id, not a sequence
	sequences := []string{
		"ducklake_snapshot_id_seq",
		"ducklake_schema_id_seq",
		"ducklake_table_id_seq",
		"ducklake_column_id_seq",
	}
	for _, seqName := range sequences {
		_, err = c.db.ExecContext(ctx, fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s.%s", c.schemaName, seqName))
		if err != nil {
			return fmt.Errorf("failed to create sequence %s: %w", seqName, err)
		}
	}

	// Create tables in dependency order matching DuckDB's DuckLake implementation
	// Each entry specifies the table name, template, and number of schema name placeholders
	tables := []struct {
		name        string
		template    string
		schemaCount int
	}{
		{"ducklake_metadata", sqltemplates.DuckLakeMetadataTable, 1},
		{"ducklake_snapshot", sqltemplates.DuckLakeSnapshotTable, 1},
		{"ducklake_snapshot_changes", sqltemplates.DuckLakeSnapshotChangesTable, 1},
		{"ducklake_schema", sqltemplates.DuckLakeSchemaTable, 1},
		{"ducklake_table", sqltemplates.DuckLakeTableTable, 1},
		{"ducklake_view", sqltemplates.DuckLakeViewTable, 1},
		{"ducklake_tag", sqltemplates.DuckLakeTagTable, 1},
		{"ducklake_column_tag", sqltemplates.DuckLakeColumnTagTable, 1},
		{"ducklake_data_file", sqltemplates.DuckLakeDataFileTable, 1},
		{"ducklake_file_column_stats", sqltemplates.DuckLakeFileColumnStatsTable, 1},
		{"ducklake_delete_file", sqltemplates.DuckLakeDeleteFileTable, 1},
		{"ducklake_column", sqltemplates.DuckLakeColumnTable, 1},
		{"ducklake_table_stats", sqltemplates.DuckLakeTableStatsTable, 1},
		{"ducklake_table_column_stats", sqltemplates.DuckLakeTableColumnStatsTable, 1},
		{"ducklake_partition_info", sqltemplates.DuckLakePartitionInfoTable, 1},
		{"ducklake_partition_column", sqltemplates.DuckLakePartitionColumnTable, 1},
		{"ducklake_file_partition_value", sqltemplates.DuckLakeFilePartitionValueTable, 1},
		{"ducklake_files_scheduled_for_deletion", sqltemplates.DuckLakeFilesScheduledForDeletionTable, 1},
		{"ducklake_inlined_data_tables", sqltemplates.DuckLakeInlinedDataTablesTable, 1},
		{"ducklake_column_mapping", sqltemplates.DuckLakeColumnMappingTable, 1},
		{"ducklake_name_mapping", sqltemplates.DuckLakeNameMappingTable, 1},
		{"ducklake_schema_versions", sqltemplates.DuckLakeSchemaVersionsTable, 1},
	}

	for _, table := range tables {
		c.logger.Debug("creating table", zap.String("table", table.name))

		// Build arguments slice with the correct number of schema names
		args := make([]any, table.schemaCount)
		for i := range args {
			args[i] = c.schemaName
		}

		// Format the SQL template with schema name(s)
		sql := fmt.Sprintf(table.template, args...)

		_, err := c.db.ExecContext(ctx, sql)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", table.name, err)
		}

		c.logger.Debug("created table", zap.String("table", table.name))
	}

	c.logger.Info("successfully created ducklake metadata schema")

	// Initialize catalog metadata if this is a new catalog
	if err := c.InitializeCatalog(ctx, dataPath); err != nil {
		return fmt.Errorf("failed to initialize catalog: %w", err)
	}

	return nil
}

// TableExists checks if a table exists in the schema.
func (c *Client) TableExists(ctx context.Context, tableName string) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = $1
			AND table_name = $2
		)
	`

	var exists bool
	err := c.db.QueryRowContext(ctx, query, c.schemaName, tableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if table exists: %w", err)
	}

	return exists, nil
}

// SchemaExists checks if the schema exists.
func (c *Client) SchemaExists(ctx context.Context) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.schemata
			WHERE schema_name = $1
		)
	`

	var exists bool
	err := c.db.QueryRowContext(ctx, query, c.schemaName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if schema exists: %w", err)
	}

	return exists, nil
}

// InitializeCatalog initializes the DuckLake catalog with bootstrap metadata.
// This creates the initial snapshot (snapshot_id=0) and metadata entries that
// DuckDB's ducklake extension expects.
func (c *Client) InitializeCatalog(ctx context.Context, dataPath string) error {
	c.logger.Info("InitializeCatalog called")

	// Check if catalog is already initialized by looking for snapshot 0
	var count int
	checkQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s.ducklake_snapshot WHERE snapshot_id = 0", c.schemaName)
	err := c.db.QueryRowContext(ctx, checkQuery).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check if catalog is initialized: %w", err)
	}

	if count > 0 {
		c.logger.Debug("catalog already initialized, skipping")
		return nil
	}

	c.logger.Info("initializing ducklake catalog with bootstrap metadata")

	// Ensure data_path has a trailing slash for proper path concatenation
	if !strings.HasSuffix(dataPath, "/") {
		dataPath = dataPath + "/"
	}

	// Insert initial metadata
	metadataRows := []struct {
		key   string
		value string
	}{
		{"version", "0.3"},
		{"created_by", "OpenTelemetry Collector DuckLake Exporter"},
		{"data_path", dataPath},
		{"encrypted", "false"},
	}

	for _, row := range metadataRows {
		insertMetadata := fmt.Sprintf(
			"INSERT INTO %s.ducklake_metadata (key, value) VALUES ($1, $2)",
			c.schemaName,
		)
		_, err := c.db.ExecContext(ctx, insertMetadata, row.key, row.value)
		if err != nil {
			return fmt.Errorf("failed to insert metadata %s: %w", row.key, err)
		}
	}

	// Insert initial snapshot (snapshot_id=0)
	insertSnapshot := fmt.Sprintf(
		"INSERT INTO %s.ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) VALUES (0, NOW(), 0, 1, 0)",
		c.schemaName,
	)
	_, err = c.db.ExecContext(ctx, insertSnapshot)
	if err != nil {
		return fmt.Errorf("failed to insert initial snapshot: %w", err)
	}

	// Insert initial schema version
	insertSchemaVersion := fmt.Sprintf(
		"INSERT INTO %s.ducklake_schema_versions (begin_snapshot, schema_version) VALUES (0, 0)",
		c.schemaName,
	)
	_, err = c.db.ExecContext(ctx, insertSchemaVersion)
	if err != nil {
		return fmt.Errorf("failed to insert schema version: %w", err)
	}

	// Insert the "main" DuckLake schema (schema_id=0)
	// This is the default schema in DuckLake, distinct from the PostgreSQL schema
	// Generate a UUID for the schema
	schemaUUID := uuid.New()
	insertMainSchema := fmt.Sprintf(
		"INSERT INTO %s.ducklake_schema (schema_id, schema_uuid, schema_name, path, path_is_relative, begin_snapshot) VALUES (0, $1, 'main', 'main/', true, 0)",
		c.schemaName,
	)
	_, err = c.db.ExecContext(ctx, insertMainSchema, schemaUUID)
	if err != nil {
		return fmt.Errorf("failed to insert main schema: %w", err)
	}

	// Log the change in snapshot_changes
	insertSchemaChange := fmt.Sprintf(
		"INSERT INTO %s.ducklake_snapshot_changes (snapshot_id, changes_made, author, commit_message) VALUES (0, 'created_schema:main', 'OpenTelemetry Collector DuckLake Exporter', 'Bootstrap catalog with main schema')",
		c.schemaName,
	)
	_, err = c.db.ExecContext(ctx, insertSchemaChange)
	if err != nil {
		return fmt.Errorf("failed to insert schema change: %w", err)
	}

	c.logger.Info("catalog initialized successfully")
	return nil
}
