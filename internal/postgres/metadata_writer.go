// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package postgres // import "github.com/hrl20/otel-ducklakeexporter/internal/postgres"

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/hrl20/otel-ducklakeexporter/internal/parquet"
)

// MetadataWriter handles writing DuckLake metadata to PostgreSQL.
type MetadataWriter struct {
	client             *Client
	duckLakeSchemaName string
	tableName          string
	tableType          string // "logs" or "traces"
	logger             *zap.Logger

	// Path translation for containerized environments
	outputPath string // Actual path where files are written (container path)
	dataPath   string // Path to register in metadata (host path)

	// Cached IDs to avoid repeated lookups
	schemaID  int64
	tableID   int64
	columnIDs map[string]int64
	mappingID int64
}

// NewMetadataWriter creates a new metadata writer.
func NewMetadataWriter(client *Client, duckLakeSchemaName string, tableName string, tableType string, outputPath string, dataPath string, logger *zap.Logger) *MetadataWriter {
	return &MetadataWriter{
		client:             client,
		duckLakeSchemaName: duckLakeSchemaName,
		tableName:          tableName,
		tableType:          tableType,
		outputPath:         outputPath,
		dataPath:           dataPath,
		logger:             logger,
		columnIDs:          make(map[string]int64),
	}
}

// RegisterParquetFile registers a parquet file in the DuckLake metadata store.
func (mw *MetadataWriter) RegisterParquetFile(ctx context.Context, stats parquet.FileStats) error {
	// Translate file path from container path to host path for metadata
	translatedStats := stats
	if mw.outputPath != mw.dataPath {
		// Replace outputPath prefix with dataPath
		translatedStats.FilePath = strings.Replace(stats.FilePath, mw.outputPath, mw.dataPath, 1)
		mw.logger.Debug("translated file path",
			zap.String("original", stats.FilePath),
			zap.String("translated", translatedStats.FilePath))
	}

	// Begin transaction
	tx, err := mw.client.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Track what catalog entries were created for snapshot changes
	var createdSchema bool
	var createdTable bool
	var createdColumns bool

	// Ensure schema exists (with caching)
	if mw.schemaID == 0 {
		var wasCreated bool
		mw.schemaID, wasCreated, err = mw.ensureSchema(ctx, tx)
		if err != nil {
			return fmt.Errorf("failed to ensure schema: %w", err)
		}
		createdSchema = wasCreated
	}

	// Ensure table exists (with caching)
	if mw.tableID == 0 {
		var wasCreated bool
		mw.tableID, wasCreated, err = mw.ensureTable(ctx, tx, mw.schemaID)
		if err != nil {
			return fmt.Errorf("failed to ensure table: %w", err)
		}
		createdTable = wasCreated
	}

	// Ensure columns exist (with caching)
	if len(mw.columnIDs) == 0 {
		var wasCreated bool
		mw.columnIDs, wasCreated, err = mw.ensureColumns(ctx, tx, mw.tableID)
		if err != nil {
			return fmt.Errorf("failed to ensure columns: %w", err)
		}
		createdColumns = wasCreated
	}

	// Create new snapshot and get the file ID to use
	snapshotID, fileID, err := mw.createSnapshot(ctx, tx, mw.tableID, createdSchema, createdTable, createdColumns)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Insert data file record with the file ID from the snapshot
	dataFileID, err := mw.insertDataFile(ctx, tx, snapshotID, fileID, translatedStats)
	if err != nil {
		return fmt.Errorf("failed to insert data file: %w", err)
	}

	// Insert column statistics
	if err := mw.insertColumnStats(ctx, tx, dataFileID, translatedStats.ColumnStats); err != nil {
		return fmt.Errorf("failed to insert column stats: %w", err)
	}

	// Update table stats
	if err := mw.updateTableStats(ctx, tx, snapshotID, translatedStats); err != nil {
		return fmt.Errorf("failed to update table stats: %w", err)
	}

	// Update table column stats
	if err := mw.updateTableColumnStats(ctx, tx, translatedStats.ColumnStats); err != nil {
		return fmt.Errorf("failed to update table column stats: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	mw.logger.Info("registered parquet file",
		zap.String("file", translatedStats.FilePath),
		zap.Int64("snapshot_id", snapshotID),
		zap.Int64("records", translatedStats.RecordCount))

	return nil
}

// ensureSchema ensures the DuckLake schema exists and returns its ID and whether it was created.
func (mw *MetadataWriter) ensureSchema(ctx context.Context, tx *sql.Tx) (int64, bool, error) {
	// Try to get existing schema where end_snapshot is NULL (current version)
	query := fmt.Sprintf("SELECT schema_id FROM %s.ducklake_schema WHERE schema_name = $1 AND end_snapshot IS NULL", mw.client.schemaName)
	var schemaID int64
	err := tx.QueryRowContext(ctx, query, mw.duckLakeSchemaName).Scan(&schemaID)

	if err == nil {
		return schemaID, false, nil
	}

	if err != sql.ErrNoRows {
		return 0, false, err
	}

	// Schema doesn't exist, create it with a new ID from sequence
	sequenceQuery := fmt.Sprintf("SELECT nextval('%s.ducklake_schema_id_seq')", mw.client.schemaName)
	err = tx.QueryRowContext(ctx, sequenceQuery).Scan(&schemaID)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get next schema_id: %w", err)
	}

	// Insert new schema
	insertQuery := fmt.Sprintf(
		"INSERT INTO %s.ducklake_schema (schema_id, schema_name, begin_snapshot) VALUES ($1, $2, 1)",
		mw.client.schemaName,
	)
	_, err = tx.ExecContext(ctx, insertQuery, schemaID, mw.duckLakeSchemaName)
	if err != nil {
		return 0, false, fmt.Errorf("failed to insert schema: %w", err)
	}

	mw.logger.Debug("created ducklake schema", zap.String("schema", mw.duckLakeSchemaName), zap.Int64("schema_id", schemaID))

	return schemaID, true, nil
}

// ensureTable ensures the DuckLake table exists and returns its ID and whether it was created.
func (mw *MetadataWriter) ensureTable(ctx context.Context, tx *sql.Tx, schemaID int64) (int64, bool, error) {
	// Try to get existing table where end_snapshot is NULL (current version)
	query := fmt.Sprintf("SELECT table_id FROM %s.ducklake_table WHERE schema_id = $1 AND table_name = $2 AND end_snapshot IS NULL", mw.client.schemaName)
	var tableID int64
	err := tx.QueryRowContext(ctx, query, schemaID, mw.tableName).Scan(&tableID)

	if err == nil {
		return tableID, false, nil
	}

	if err != sql.ErrNoRows {
		return 0, false, err
	}

	// Table doesn't exist, create it with a new ID from sequence
	sequenceQuery := fmt.Sprintf("SELECT nextval('%s.ducklake_table_id_seq')", mw.client.schemaName)
	err = tx.QueryRowContext(ctx, sequenceQuery).Scan(&tableID)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get next table_id: %w", err)
	}

	// Generate UUID for the table
	tableUUID := uuid.New()
	tablePath := mw.tableName + "/"

	// Insert new table with UUID and path
	insertQuery := fmt.Sprintf(
		"INSERT INTO %s.ducklake_table (table_id, table_uuid, schema_id, table_name, path, path_is_relative, begin_snapshot) VALUES ($1, $2, $3, $4, $5, true, 1)",
		mw.client.schemaName,
	)
	_, err = tx.ExecContext(ctx, insertQuery, tableID, tableUUID, schemaID, mw.tableName, tablePath)
	if err != nil {
		return 0, false, fmt.Errorf("failed to insert table: %w", err)
	}

	mw.logger.Debug("created ducklake table", zap.String("table", mw.tableName), zap.Int64("table_id", tableID))

	return tableID, true, nil
}

// ensureColumns ensures the DuckLake columns exist and returns their IDs and whether any were created.
func (mw *MetadataWriter) ensureColumns(ctx context.Context, tx *sql.Tx, tableID int64) (map[string]int64, bool, error) {
	columnIDs := make(map[string]int64)
	var createdAny bool

	// Get column names and types from the parquet schema based on table type
	var columnNames []string
	var columnTypes []string
	if mw.tableType == "traces" {
		columnNames = parquet.GetSpanColumnNames()
		columnTypes = parquet.GetSpanColumnTypes()
	} else {
		columnNames = parquet.GetColumnNames()
		columnTypes = parquet.GetColumnTypes()
	}

	for i, columnName := range columnNames {
		// Try to get existing column where end_snapshot is NULL (current version)
		query := fmt.Sprintf("SELECT column_id FROM %s.ducklake_column WHERE table_id = $1 AND column_name = $2 AND end_snapshot IS NULL", mw.client.schemaName)
		var columnID int64
		err := tx.QueryRowContext(ctx, query, tableID, columnName).Scan(&columnID)

		if err == nil {
			columnIDs[columnName] = columnID
			continue
		}

		if err != sql.ErrNoRows {
			return nil, false, err
		}

		// Column doesn't exist, create it with a new ID from sequence
		sequenceQuery := fmt.Sprintf("SELECT nextval('%s.ducklake_column_id_seq')", mw.client.schemaName)
		err = tx.QueryRowContext(ctx, sequenceQuery).Scan(&columnID)
		if err != nil {
			return nil, false, fmt.Errorf("failed to get next column_id: %w", err)
		}

		// Insert new column
		// Note: column_order starts at 1, not 0
		insertQuery := fmt.Sprintf(
			"INSERT INTO %s.ducklake_column (column_id, table_id, column_name, column_type, column_order, begin_snapshot, nulls_allowed) VALUES ($1, $2, $3, $4, $5, 1, $6)",
			mw.client.schemaName,
		)

		_, err = tx.ExecContext(ctx, insertQuery, columnID, tableID, columnName, columnTypes[i], i+1, true)
		if err != nil {
			return nil, false, fmt.Errorf("failed to insert column %s: %w", columnName, err)
		}

		columnIDs[columnName] = columnID
		createdAny = true

		mw.logger.Debug("created ducklake column",
			zap.String("column", columnName),
			zap.Int64("column_id", columnID))
	}

	// If we created columns, ensure column mapping exists
	if createdAny {
		if err := mw.ensureColumnMapping(ctx, tx, tableID, columnIDs); err != nil {
			return nil, false, fmt.Errorf("failed to ensure column mapping: %w", err)
		}
	}

	return columnIDs, createdAny, nil
}

// ensureColumnMapping ensures the column mapping and name mappings exist for the table.
func (mw *MetadataWriter) ensureColumnMapping(ctx context.Context, tx *sql.Tx, tableID int64, columnIDs map[string]int64) error {
	// Check if column_mapping already exists for this table
	checkQuery := fmt.Sprintf("SELECT mapping_id FROM %s.ducklake_column_mapping WHERE table_id = $1", mw.client.schemaName)
	var existingMappingID int64
	err := tx.QueryRowContext(ctx, checkQuery, tableID).Scan(&existingMappingID)

	if err == nil {
		// Mapping already exists, cache it and return
		mw.mappingID = existingMappingID
		return nil
	}

	if err != sql.ErrNoRows {
		return fmt.Errorf("failed to check existing column mapping: %w", err)
	}

	// Get next_catalog_id from the latest snapshot for the mapping_id
	snapshotQuery := fmt.Sprintf("SELECT COALESCE(MAX(next_catalog_id), 1) FROM %s.ducklake_snapshot", mw.client.schemaName)
	var mappingID int64
	err = tx.QueryRowContext(ctx, snapshotQuery).Scan(&mappingID)
	if err != nil {
		return fmt.Errorf("failed to get next_catalog_id for mapping: %w", err)
	}

	// Insert column_mapping entry with type="map_by_name"
	insertMappingQuery := fmt.Sprintf(
		"INSERT INTO %s.ducklake_column_mapping (mapping_id, table_id, type) VALUES ($1, $2, $3)",
		mw.client.schemaName,
	)
	_, err = tx.ExecContext(ctx, insertMappingQuery, mappingID, tableID, "map_by_name")
	if err != nil {
		return fmt.Errorf("failed to insert column mapping: %w", err)
	}

	// Cache the mapping_id for use in data file registration
	mw.mappingID = mappingID

	mw.logger.Debug("created column mapping",
		zap.Int64("mapping_id", mappingID),
		zap.Int64("table_id", tableID))

	// Create name_mapping entries for each column
	// Map parquet column index (0-based) to DuckLake column_id
	var columnNames []string
	if mw.tableType == "traces" {
		columnNames = parquet.GetSpanColumnNames()
	} else {
		columnNames = parquet.GetColumnNames()
	}
	for parquetColumnIndex, columnName := range columnNames {
		duckLakeColumnID, ok := columnIDs[columnName]
		if !ok {
			return fmt.Errorf("column %s not found in columnIDs map", columnName)
		}

		insertNameMappingQuery := fmt.Sprintf(
			"INSERT INTO %s.ducklake_name_mapping (mapping_id, column_id, source_name, target_field_id, parent_column, is_partition) VALUES ($1, $2, $3, $4, NULL, $5)",
			mw.client.schemaName,
		)
		_, err = tx.ExecContext(ctx, insertNameMappingQuery, mappingID, parquetColumnIndex, columnName, duckLakeColumnID, false)
		if err != nil {
			return fmt.Errorf("failed to insert name mapping for column %s: %w", columnName, err)
		}

		mw.logger.Debug("created name mapping",
			zap.String("column", columnName),
			zap.Int("parquet_index", parquetColumnIndex),
			zap.Int64("ducklake_column_id", duckLakeColumnID))
	}

	return nil
}

// createSnapshot creates a new snapshot and returns its ID and the file ID to use for the data file.
func (mw *MetadataWriter) createSnapshot(ctx context.Context, tx *sql.Tx, tableID int64, createdSchema bool, createdTable bool, createdColumns bool) (int64, int64, error) {
	// Get next snapshot ID from sequence (atomic, no locking needed)
	var snapshotID int64
	sequenceQuery := fmt.Sprintf(`SELECT nextval('%s.ducklake_snapshot_id_seq')`, mw.client.schemaName)
	err := tx.QueryRowContext(ctx, sequenceQuery).Scan(&snapshotID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get next snapshot ID: %w", err)
	}

	// Get current snapshot state for other fields
	query := fmt.Sprintf(`
		SELECT
			COALESCE(MAX(schema_version), 0) as schema_version,
			COALESCE(MAX(next_catalog_id), 1) as next_catalog_id,
			COALESCE(MAX(next_file_id), 0) as next_file_id
		FROM %s.ducklake_snapshot
	`, mw.client.schemaName)

	var schemaVersion, nextCatalogID, currentNextFileID int64
	err = tx.QueryRowContext(ctx, query).Scan(&schemaVersion, &nextCatalogID, &currentNextFileID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get snapshot state: %w", err)
	}

	// The file ID to use for the data file we're about to insert (current next_file_id)
	fileID := currentNextFileID

	// The next_file_id for the snapshot (will be used by the next file)
	nextFileID := currentNextFileID + 1

	// Increment next_catalog_id and schema_version if we created new catalog entries (schema/table/columns)
	createdNewCatalogEntries := createdSchema || createdTable || createdColumns
	if createdNewCatalogEntries {
		nextCatalogID++
		schemaVersion++
	}

	// Insert new snapshot with all required fields
	insertSnapshot := fmt.Sprintf(`
		INSERT INTO %s.ducklake_snapshot (
			snapshot_id,
			snapshot_time,
			schema_version,
			next_catalog_id,
			next_file_id
		) VALUES ($1, NOW(), $2, $3, $4)
	`, mw.client.schemaName)

	_, err = tx.ExecContext(ctx, insertSnapshot, snapshotID, schemaVersion, nextCatalogID, nextFileID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Log snapshot changes
	insertChanges := fmt.Sprintf(`
		INSERT INTO %s.ducklake_snapshot_changes (
			snapshot_id,
			changes_made,
			author,
			commit_message,
			commit_extra_info
		) VALUES ($1, $2, $3, $4, NULL)
	`, mw.client.schemaName)

	// Build spec-compliant changes_made string
	var changesList []string

	if createdSchema {
		changesList = append(changesList, fmt.Sprintf("created_schema:%s", mw.duckLakeSchemaName))
	}
	if createdTable {
		changesList = append(changesList, fmt.Sprintf("created_table:%s", mw.tableName))
	}
	// Always add inserted_into_table since we're adding a data file
	changesList = append(changesList, fmt.Sprintf("inserted_into_table:%d", tableID))

	changes := strings.Join(changesList, ",")
	author := "OpenTelemetry Collector DuckLake Exporter"
	commitMsg := fmt.Sprintf("Added data file for table %s (table_id=%d)", mw.tableName, tableID)

	_, err = tx.ExecContext(ctx, insertChanges, snapshotID, changes, author, commitMsg)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to log snapshot changes: %w", err)
	}

	mw.logger.Debug("created snapshot",
		zap.Int64("snapshot_id", snapshotID),
		zap.Int64("schema_version", schemaVersion),
		zap.Int64("next_catalog_id", nextCatalogID),
		zap.Int64("next_file_id", nextFileID),
		zap.Int64("file_id", fileID))

	return snapshotID, fileID, nil
}

// insertDataFile inserts a data file record and returns its ID.
// The fileID parameter is the data_file_id to use, which comes from the snapshot's next_file_id tracking.
func (mw *MetadataWriter) insertDataFile(ctx context.Context, tx *sql.Tx, snapshotID int64, fileID int64, stats parquet.FileStats) (int64, error) {
	// Use the file ID from the snapshot's next_file_id tracking
	dataFileID := fileID

	// Get file_order for this table
	fileOrderQuery := fmt.Sprintf("SELECT COALESCE(MAX(file_order), -1) + 1 FROM %s.ducklake_data_file WHERE table_id = $1", mw.client.schemaName)
	var fileOrder int64
	var err error
	err = tx.QueryRowContext(ctx, fileOrderQuery, mw.tableID).Scan(&fileOrder)
	if err != nil {
		return 0, fmt.Errorf("failed to get next file_order: %w", err)
	}

	// Extract just the filename (relative to table directory) from the full path
	// The filename is relative to the table path (schema/table/)
	filename := filepath.Base(stats.FilePath)

	// Insert new data file with DuckDB schema, including mapping_id for column mapping
	query := fmt.Sprintf(
		"INSERT INTO %s.ducklake_data_file (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, partial_file_info, mapping_id) VALUES ($1, $2, $3, NULL, $4, $5, $6, $7, $8, $9, $10, NULL, NULL, NULL, NULL, $11)",
		mw.client.schemaName,
	)

	_, err = tx.ExecContext(ctx, query, dataFileID, mw.tableID, snapshotID, fileOrder, filename, true, "PARQUET", stats.RecordCount, stats.FileSizeBytes, stats.FooterSize, mw.mappingID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert data file: %w", err)
	}

	mw.logger.Debug("inserted data file",
		zap.Int64("data_file_id", dataFileID),
		zap.String("filename", filename),
		zap.String("full_path", stats.FilePath))

	return dataFileID, nil
}

// insertColumnStats inserts column statistics for a file.
func (mw *MetadataWriter) insertColumnStats(ctx context.Context, tx *sql.Tx, dataFileID int64, columnStats []parquet.ColumnStats) error {
	if len(columnStats) == 0 {
		return nil
	}

	for _, stats := range columnStats {
		// Get column ID
		columnID, ok := mw.columnIDs[stats.ColumnName]
		if !ok {
			mw.logger.Warn("column not found in cache", zap.String("column", stats.ColumnName))
			continue
		}

		query := fmt.Sprintf(
			"INSERT INTO %s.ducklake_file_column_stats (data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL)",
			mw.client.schemaName,
		)

		// value_count is the total number of values (record count)
		_, err := tx.ExecContext(ctx, query, dataFileID, mw.tableID, columnID, stats.SizeBytes, stats.DistinctCount, stats.NullCount, stats.MinValue, stats.MaxValue, false)
		if err != nil {
			return fmt.Errorf("failed to insert column stats for %s: %w", stats.ColumnName, err)
		}
	}

	mw.logger.Debug("inserted column stats", zap.Int("count", len(columnStats)))

	return nil
}

// updateTableStats updates the table statistics.
func (mw *MetadataWriter) updateTableStats(ctx context.Context, tx *sql.Tx, snapshotID int64, stats parquet.FileStats) error {
	// Check if stats already exist for this table
	query := fmt.Sprintf("SELECT record_count, next_row_id, file_size_bytes FROM %s.ducklake_table_stats WHERE table_id = $1", mw.client.schemaName)
	var existingRecords, existingNextRowID, existingSize int64
	err := tx.QueryRowContext(ctx, query, mw.tableID).Scan(&existingRecords, &existingNextRowID, &existingSize)

	if err == sql.ErrNoRows {
		// No existing stats, insert new
		insertQuery := fmt.Sprintf(
			"INSERT INTO %s.ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes) VALUES ($1, $2, $3, $4)",
			mw.client.schemaName,
		)
		_, err = tx.ExecContext(ctx, insertQuery, mw.tableID, stats.RecordCount, stats.RecordCount, stats.FileSizeBytes)
		if err != nil {
			return fmt.Errorf("failed to insert table stats: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to query table stats: %w", err)
	} else {
		// Update existing stats
		updateQuery := fmt.Sprintf(
			"UPDATE %s.ducklake_table_stats SET record_count = $1, next_row_id = $2, file_size_bytes = $3 WHERE table_id = $4",
			mw.client.schemaName,
		)
		_, err = tx.ExecContext(ctx, updateQuery, existingRecords+stats.RecordCount, existingNextRowID+stats.RecordCount, existingSize+stats.FileSizeBytes, mw.tableID)
		if err != nil {
			return fmt.Errorf("failed to update table stats: %w", err)
		}
	}

	mw.logger.Debug("updated table stats", zap.Int64("table_id", mw.tableID))

	return nil
}

// updateTableColumnStats updates the table column statistics with stats from the current parquet file.
func (mw *MetadataWriter) updateTableColumnStats(ctx context.Context, tx *sql.Tx, columnStats []parquet.ColumnStats) error {
	if len(columnStats) == 0 {
		return nil
	}

	for _, stats := range columnStats {
		// Get column ID
		columnID, ok := mw.columnIDs[stats.ColumnName]
		if !ok {
			mw.logger.Warn("column not found in cache", zap.String("column", stats.ColumnName))
			continue
		}

		containsNull := stats.NullCount > 0

		// Check if stats already exist for this column
		var existsCount int
		existsQuery := fmt.Sprintf(`
			SELECT COUNT(*) FROM %s.ducklake_table_column_stats
			WHERE table_id = $1 AND column_id = $2
		`, mw.client.schemaName)
		err := tx.QueryRowContext(ctx, existsQuery, mw.tableID, columnID).Scan(&existsCount)
		if err != nil {
			return fmt.Errorf("failed to check table column stats existence: %w", err)
		}

		if existsCount == 0 {
			// Insert new stats from this parquet file
			insertQuery := fmt.Sprintf(`
				INSERT INTO %s.ducklake_table_column_stats
				(table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats)
				VALUES ($1, $2, $3, $4, $5, $6, NULL)
			`, mw.client.schemaName)
			_, err = tx.ExecContext(ctx, insertQuery, mw.tableID, columnID, containsNull, false, stats.MinValue, stats.MaxValue)
			if err != nil {
				return fmt.Errorf("failed to insert table column stats for %s: %w", stats.ColumnName, err)
			}
		} else {
			// Get existing min/max values to compute the overall min/max
			var existingContainsNull bool
			var existingMinValue, existingMaxValue string
			getQuery := fmt.Sprintf(`
				SELECT contains_null, min_value, max_value
				FROM %s.ducklake_table_column_stats
				WHERE table_id = $1 AND column_id = $2
			`, mw.client.schemaName)
			err = tx.QueryRowContext(ctx, getQuery, mw.tableID, columnID).Scan(&existingContainsNull, &existingMinValue, &existingMaxValue)
			if err != nil {
				return fmt.Errorf("failed to get existing table column stats for %s: %w", stats.ColumnName, err)
			}

			// Compute the overall min/max across all files
			newMinValue := existingMinValue
			newMaxValue := existingMaxValue

			// Update min if current file has a smaller min (and both are non-empty)
			if stats.MinValue != "" && (existingMinValue == "" || stats.MinValue < existingMinValue) {
				newMinValue = stats.MinValue
			}

			// Update max if current file has a larger max (and both are non-empty)
			if stats.MaxValue != "" && (existingMaxValue == "" || stats.MaxValue > existingMaxValue) {
				newMaxValue = stats.MaxValue
			}

			// Update contains_null if either existing or new file has nulls
			newContainsNull := existingContainsNull || containsNull

			// Update with computed min/max
			updateQuery := fmt.Sprintf(`
				UPDATE %s.ducklake_table_column_stats
				SET contains_null = $1, min_value = $2, max_value = $3
				WHERE table_id = $4 AND column_id = $5
			`, mw.client.schemaName)
			_, err = tx.ExecContext(ctx, updateQuery, newContainsNull, newMinValue, newMaxValue, mw.tableID, columnID)
			if err != nil {
				return fmt.Errorf("failed to update table column stats for %s: %w", stats.ColumnName, err)
			}
		}
	}

	mw.logger.Debug("updated table column stats", zap.Int("count", len(columnStats)))

	return nil
}
