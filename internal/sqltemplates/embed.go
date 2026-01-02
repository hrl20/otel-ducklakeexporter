// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sqltemplates // import "github.com/hrl20/otel-ducklakeexporter/internal/sqltemplates"

import _ "embed"

//go:embed ducklake_metadata.sql
var DuckLakeMetadataTable string

//go:embed ducklake_snapshot.sql
var DuckLakeSnapshotTable string

//go:embed ducklake_snapshot_changes.sql
var DuckLakeSnapshotChangesTable string

//go:embed ducklake_schema.sql
var DuckLakeSchemaTable string

//go:embed ducklake_table.sql
var DuckLakeTableTable string

//go:embed ducklake_view.sql
var DuckLakeViewTable string

//go:embed ducklake_tag.sql
var DuckLakeTagTable string

//go:embed ducklake_column_tag.sql
var DuckLakeColumnTagTable string

//go:embed ducklake_data_file.sql
var DuckLakeDataFileTable string

//go:embed ducklake_file_column_stats.sql
var DuckLakeFileColumnStatsTable string

//go:embed ducklake_delete_file.sql
var DuckLakeDeleteFileTable string

//go:embed ducklake_column.sql
var DuckLakeColumnTable string

//go:embed ducklake_table_stats.sql
var DuckLakeTableStatsTable string

//go:embed ducklake_table_column_stats.sql
var DuckLakeTableColumnStatsTable string

//go:embed ducklake_partition_info.sql
var DuckLakePartitionInfoTable string

//go:embed ducklake_partition_column.sql
var DuckLakePartitionColumnTable string

//go:embed ducklake_file_partition_value.sql
var DuckLakeFilePartitionValueTable string

//go:embed ducklake_files_scheduled_for_deletion.sql
var DuckLakeFilesScheduledForDeletionTable string

//go:embed ducklake_inlined_data_tables.sql
var DuckLakeInlinedDataTablesTable string

//go:embed ducklake_column_mapping.sql
var DuckLakeColumnMappingTable string

//go:embed ducklake_name_mapping.sql
var DuckLakeNameMappingTable string

//go:embed ducklake_schema_versions.sql
var DuckLakeSchemaVersionsTable string
