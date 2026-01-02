// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package ducklakeexporter

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap/zaptest"

	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/lib/pq"
)

type testContext struct {
	pgContainer    testcontainers.Container
	pgHost         string
	pgPort         string
	tempDir        string
	exporter       *logsExporter
	tracesExporter *tracesExporter
	cancelFunc     context.CancelFunc
}

func setupIntegrationTest(t *testing.T) *testContext {
	ctx := context.Background()

	// Create temporary directory for parquet files
	tempDir, err := os.MkdirTemp("", "ducklake-test-*")
	require.NoError(t, err)

	// Check for custom PostgreSQL port from environment variable
	// If DUCKLAKE_TEST_PG_PORT is set, use that port on the host
	// Otherwise, let testcontainers pick a random available port
	var exposedPorts []string
	customPort := os.Getenv("DUCKLAKE_TEST_PG_PORT")
	if customPort != "" {
		// Bind container port 5432 to the specified host port
		exposedPorts = []string{customPort + ":5432/tcp"}
		t.Logf("Using custom PostgreSQL port from DUCKLAKE_TEST_PG_PORT: %s", customPort)
	} else {
		// Let testcontainers pick a random available port
		exposedPorts = []string{"5432/tcp"}
	}

	// Start PostgreSQL container
	pgReq := testcontainers.ContainerRequest{
		Image:        "postgres:16",
		ExposedPorts: exposedPorts,
		Env: map[string]string{
			"POSTGRES_DB":       "ducklake_test",
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}

	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: pgReq,
		Started:          true,
	})
	require.NoError(t, err)

	// Get PostgreSQL host and port
	pgHost, err := pgContainer.Host(ctx)
	require.NoError(t, err)

	pgMappedPort, err := pgContainer.MappedPort(ctx, "5432")
	require.NoError(t, err)

	return &testContext{
		pgContainer: pgContainer,
		pgHost:      pgHost,
		pgPort:      pgMappedPort.Port(),
		tempDir:     tempDir,
	}
}

func (tc *testContext) cleanup(t *testing.T) {
	ctx := context.Background()

	if tc.cancelFunc != nil {
		tc.cancelFunc()
	}

	if tc.exporter != nil {
		_ = tc.exporter.shutdown(ctx)
	}

	if tc.tracesExporter != nil {
		_ = tc.tracesExporter.shutdown(ctx)
	}

	if tc.pgContainer != nil {
		_ = tc.pgContainer.Terminate(ctx)
	}

	if tc.tempDir != "" {
		_ = os.RemoveAll(tc.tempDir)
	}
}

func (tc *testContext) createExporter(t *testing.T) {
	cfg := &Config{
		PostgreSQL: PostgreSQLConfig{
			Host:     tc.pgHost,
			Port:     mustAtoi(tc.pgPort),
			Database: "ducklake_test",
			Username: "test",
			Password: "test",
			SSLMode:  "disable",
			Schema:   "public", // PostgreSQL schema for metadata tables
		},
		Parquet: ParquetConfig{
			BatchSize:          100,
			BatchTimeout:       1 * time.Second,
			Compression:        "SNAPPY",
			EnableBloomFilters: true,
		},
		DuckLake: DuckLakeConfig{
			OutputPath:      tc.tempDir,
			LogsTableName:   "test_logs",
			TracesTableName: "test_traces",
		},
	}

	// Validate config to apply defaults (e.g., DataPath = OutputPath if not set)
	err := cfg.Validate()
	require.NoError(t, err)

	logger := zaptest.NewLogger(t)
	factory := NewFactory()
	settings := exportertest.NewNopSettings(factory.Type())
	settings.Logger = logger

	tc.exporter = newLogsExporter(logger, cfg)
	require.NotNil(t, tc.exporter)

	ctx := context.Background()
	err = tc.exporter.start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)
}

func (tc *testContext) createTracesExporter(t *testing.T) {
	cfg := &Config{
		PostgreSQL: PostgreSQLConfig{
			Host:     tc.pgHost,
			Port:     mustAtoi(tc.pgPort),
			Database: "ducklake_test",
			Username: "test",
			Password: "test",
			SSLMode:  "disable",
			Schema:   "public", // PostgreSQL schema for metadata tables
		},
		Parquet: ParquetConfig{
			BatchSize:          100,
			BatchTimeout:       1 * time.Second,
			Compression:        "SNAPPY",
			EnableBloomFilters: true,
		},
		DuckLake: DuckLakeConfig{
			OutputPath:      tc.tempDir,
			LogsTableName:   "test_logs",
			TracesTableName: "test_traces",
		},
	}

	// Validate config to apply defaults (e.g., DataPath = OutputPath if not set)
	err := cfg.Validate()
	require.NoError(t, err)

	logger := zaptest.NewLogger(t)

	tc.tracesExporter = newTracesExporter(logger, cfg)
	require.NotNil(t, tc.tracesExporter)

	ctx := context.Background()
	err = tc.tracesExporter.start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)
}

func mustAtoi(s string) int {
	var i int
	fmt.Sscanf(s, "%d", &i)
	return i
}

func TestIntegration_WriteLogsAndReadWithDuckDB(t *testing.T) {
	tc := setupIntegrationTest(t)
	defer tc.cleanup(t)

	tc.createExporter(t)

	// Generate test logs
	logs := generateTestLogs(100)

	// Export logs
	ctx := context.Background()
	err := tc.exporter.pushLogsData(ctx, logs)
	require.NoError(t, err)

	// Force flush by shutting down (which flushes pending batches)
	err = tc.exporter.shutdown(ctx)
	require.NoError(t, err)
	tc.exporter = nil

	// Verify parquet files were created
	// Files are in {tempDir}/main/{table_name}/ directory structure
	files, err := filepath.Glob(filepath.Join(tc.tempDir, "main", "test_logs", "test_logs_*.parquet"))
	require.NoError(t, err)
	require.NotEmpty(t, files, "Expected parquet files to be created")

	t.Logf("Created %d parquet file(s): %v", len(files), files)

	// Verify PostgreSQL metadata
	tc.verifyPostgreSQLMetadata(t)

	// Verify data readable by DuckDB with ducklake extension
	tc.verifyDuckDBRead(t)
}

func (tc *testContext) verifyPostgreSQLMetadata(t *testing.T) {
	dsn := fmt.Sprintf("host=%s port=%s dbname=ducklake_test user=test password=test sslmode=disable",
		tc.pgHost, tc.pgPort)

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Verify schema exists
	var schemaExists bool
	err = db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM public.ducklake_schema WHERE schema_name = 'main')").
		Scan(&schemaExists)
	require.NoError(t, err)
	assert.True(t, schemaExists, "main schema should exist")

	// Verify table exists
	var tableExists bool
	err = db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM public.ducklake_table t
			JOIN public.ducklake_schema s ON t.schema_id = s.schema_id
			WHERE s.schema_name = 'main' AND t.table_name = 'test_logs'
		)
	`).Scan(&tableExists)
	require.NoError(t, err)
	assert.True(t, tableExists, "test_logs table should exist")

	// Verify snapshot was created with all required fields
	var snapshotCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.ducklake_snapshot").Scan(&snapshotCount)
	require.NoError(t, err)
	assert.Greater(t, snapshotCount, 0, "At least one snapshot should exist")

	// Verify no snapshots have NULL required fields
	var nullFieldCount int
	err = db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM public.ducklake_snapshot
		WHERE schema_version IS NULL
		   OR next_catalog_id IS NULL
		   OR next_file_id IS NULL
	`).Scan(&nullFieldCount)
	require.NoError(t, err)
	assert.Equal(t, 0, nullFieldCount, "No snapshots should have NULL required fields")

	// Verify snapshot_changes entries exist
	var changesCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.ducklake_snapshot_changes").Scan(&changesCount)
	require.NoError(t, err)
	assert.Greater(t, changesCount, 0, "At least one snapshot_changes entry should exist")

	// Verify data files were registered
	var dataFileCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.ducklake_data_file").Scan(&dataFileCount)
	require.NoError(t, err)
	assert.Greater(t, dataFileCount, 0, "At least one data file should be registered")

	// Verify record count in table stats
	var totalRecords int64
	err = db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(record_count), 0)
		FROM public.ducklake_table_stats
	`).Scan(&totalRecords)
	require.NoError(t, err)
	assert.Equal(t, int64(100), totalRecords, "Should have 100 records in table stats")

	t.Logf("PostgreSQL metadata verification passed: %d snapshots, %d data files, %d total records",
		snapshotCount, dataFileCount, totalRecords)
}

func (tc *testContext) verifyDuckDBRead(t *testing.T) {
	// Open DuckDB in-memory database
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()

	// Install and load ducklake extension
	_, err = db.ExecContext(ctx, "INSTALL ducklake")
	require.NoError(t, err, "DuckDB ducklake extension must be available for integration test")

	_, err = db.ExecContext(ctx, "LOAD ducklake")
	require.NoError(t, err)

	// Configure DuckDB to use our PostgreSQL metadata store
	pgDSN := fmt.Sprintf("host=%s port=%s dbname=ducklake_test user=test password=test",
		tc.pgHost, tc.pgPort)

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		ATTACH 'ducklake:postgres:%s' AS otel;
	`, pgDSN))
	require.NoError(t, err)

	// Query data using ducklake
	query := "SELECT COUNT(*) as count FROM otel.main.test_logs"
	var count int
	err = db.QueryRowContext(ctx, query).Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, 100, count, "Should be able to read 100 records via DuckDB ducklake extension")

	// Query specific fields
	query = `
		SELECT
			COUNT(*) as total,
			MIN(timestamp) as min_time,
			MAX(timestamp) as max_time
		FROM otel.main.test_logs
	`

	var total int
	var minTime, maxTime time.Time
	err = db.QueryRowContext(ctx, query).Scan(&total, &minTime, &maxTime)
	require.NoError(t, err)

	assert.Equal(t, 100, total)
	assert.False(t, minTime.IsZero(), "Min time should not be zero")
	assert.False(t, maxTime.IsZero(), "Max time should not be zero")

	t.Logf("DuckDB verification passed: %d total records, time range: %v to %v",
		total, minTime, maxTime)
}

func TestIntegration_DirectParquetRead(t *testing.T) {
	tc := setupIntegrationTest(t)
	defer tc.cleanup(t)

	tc.createExporter(t)

	// Generate test logs
	logs := generateTestLogs(50)

	// Export logs
	ctx := context.Background()
	err := tc.exporter.pushLogsData(ctx, logs)
	require.NoError(t, err)

	// Shutdown to flush
	err = tc.exporter.shutdown(ctx)
	require.NoError(t, err)
	tc.exporter = nil

	// Open DuckDB and read parquet files directly
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Read parquet files directly without ducklake extension
	parquetPattern := filepath.Join(tc.tempDir, "main", "test_logs", "test_logs_*.parquet")
	query := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", parquetPattern)

	var count int
	err = db.QueryRowContext(ctx, query).Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, 50, count, "Should be able to read 50 records directly from parquet")

	// Verify schema
	query = fmt.Sprintf(`
		SELECT
			column_name,
			column_type
		FROM (
			DESCRIBE SELECT * FROM read_parquet('%s')
		)
		ORDER BY column_name
	`, parquetPattern)

	rows, err := db.QueryContext(ctx, query)
	require.NoError(t, err)
	defer rows.Close()

	columnNames := []string{}
	for rows.Next() {
		var name, colType string
		err = rows.Scan(&name, &colType)
		require.NoError(t, err)
		columnNames = append(columnNames, name)
	}

	// Verify expected columns exist
	expectedColumns := []string{
		"timestamp", "trace_id", "span_id", "severity_number", "severity_text",
		"body", "scope_name", "scope_version", "resource_attributes",
		"log_attributes", "flags", "observed_timestamp",
	}

	for _, expected := range expectedColumns {
		assert.Contains(t, columnNames, expected, "Column %s should exist", expected)
	}

	t.Logf("Direct parquet read verification passed: %d records, %d columns", count, len(columnNames))
}

func generateTestLogs(count int) plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()

	// Add resource attributes
	resource := resourceLogs.Resource()
	resource.Attributes().PutStr("service.name", "test-service")
	resource.Attributes().PutStr("deployment.environment", "test")
	resource.Attributes().PutStr("host.name", "test-host")

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scope := scopeLogs.Scope()
	scope.SetName("test-scope")
	scope.SetVersion("1.0.0")

	baseTime := time.Now().Add(-1 * time.Hour)

	for i := 0; i < count; i++ {
		logRecord := scopeLogs.LogRecords().AppendEmpty()

		// Set timestamp
		timestamp := baseTime.Add(time.Duration(i) * time.Second)
		logRecord.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
		logRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(timestamp))

		// Set trace and span IDs for some records
		if i%5 == 0 {
			logRecord.SetTraceID(pcommon.TraceID([16]byte{
				byte(i), byte(i >> 8), 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0,
			}))
			logRecord.SetSpanID(pcommon.SpanID([8]byte{
				byte(i), byte(i >> 8), 0, 0, 0, 0, 0, 0,
			}))
		}

		// Set severity
		severities := []plog.SeverityNumber{
			plog.SeverityNumberInfo,
			plog.SeverityNumberWarn,
			plog.SeverityNumberError,
			plog.SeverityNumberDebug,
		}
		severity := severities[i%len(severities)]
		logRecord.SetSeverityNumber(severity)
		logRecord.SetSeverityText(severity.String())

		// Set body
		logRecord.Body().SetStr(fmt.Sprintf("Test log message %d with some content", i))

		// Add log attributes
		logRecord.Attributes().PutStr("log.id", fmt.Sprintf("log-%d", i))
		logRecord.Attributes().PutInt("log.sequence", int64(i))
		logRecord.Attributes().PutBool("test.flag", i%2 == 0)

		// Set flags
		if i%10 == 0 {
			logRecord.SetFlags(plog.DefaultLogRecordFlags.WithIsSampled(true))
		}
	}

	return logs
}

func generateTestTraces(count int) ptrace.Traces {
	traces := ptrace.NewTraces()
	resourceSpans := traces.ResourceSpans().AppendEmpty()

	// Add resource attributes
	resource := resourceSpans.Resource()
	resource.Attributes().PutStr("service.name", "test-trace-service")
	resource.Attributes().PutStr("deployment.environment", "test")
	resource.Attributes().PutStr("host.name", "test-host")

	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	scope := scopeSpans.Scope()
	scope.SetName("test-scope")
	scope.SetVersion("1.0.0")

	baseTime := time.Now().Add(-1 * time.Hour)

	for i := 0; i < count; i++ {
		span := scopeSpans.Spans().AppendEmpty()

		// Generate trace ID and span ID
		traceID := pcommon.TraceID([16]byte{
			byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24),
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, byte(i % 256),
		})
		spanID := pcommon.SpanID([8]byte{
			byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24),
			0, 0, 0, byte(i % 256),
		})

		span.SetTraceID(traceID)
		span.SetSpanID(spanID)

		// Set parent span ID for some spans to create a trace tree
		if i > 0 && i%3 == 0 {
			parentSpanID := pcommon.SpanID([8]byte{
				byte(i - 1), byte((i - 1) >> 8), byte((i - 1) >> 16), byte((i - 1) >> 24),
				0, 0, 0, byte((i - 1) % 256),
			})
			span.SetParentSpanID(parentSpanID)
		}

		// Set name
		span.SetName(fmt.Sprintf("test-span-%d", i))

		// Set kind
		kinds := []ptrace.SpanKind{
			ptrace.SpanKindServer,
			ptrace.SpanKindClient,
			ptrace.SpanKindInternal,
			ptrace.SpanKindProducer,
			ptrace.SpanKindConsumer,
		}
		span.SetKind(kinds[i%len(kinds)])

		// Set timestamps
		startTime := baseTime.Add(time.Duration(i) * time.Second)
		endTime := startTime.Add(time.Duration(100+i) * time.Millisecond)
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(startTime))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(endTime))

		// Set status
		if i%7 == 0 {
			span.Status().SetCode(ptrace.StatusCodeError)
			span.Status().SetMessage("Test error")
		} else {
			span.Status().SetCode(ptrace.StatusCodeOk)
		}

		// Add span attributes
		span.Attributes().PutStr("span.id", fmt.Sprintf("span-%d", i))
		span.Attributes().PutInt("span.sequence", int64(i))
		span.Attributes().PutBool("test.flag", i%2 == 0)
		span.Attributes().PutStr("http.method", "GET")
		span.Attributes().PutStr("http.url", fmt.Sprintf("/api/test/%d", i))

		// Add an event
		if i%5 == 0 {
			event := span.Events().AppendEmpty()
			event.SetName("test-event")
			event.SetTimestamp(pcommon.NewTimestampFromTime(startTime.Add(50 * time.Millisecond)))
			event.Attributes().PutStr("event.type", "test")
		}

		// Add a link
		if i > 0 && i%4 == 0 {
			link := span.Links().AppendEmpty()
			linkedTraceID := pcommon.TraceID([16]byte{
				byte(i - 1), byte((i - 1) >> 8), 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			})
			linkedSpanID := pcommon.SpanID([8]byte{
				byte(i - 1), byte((i - 1) >> 8), 0, 0, 0, 0, 0, 0,
			})
			link.SetTraceID(linkedTraceID)
			link.SetSpanID(linkedSpanID)
			link.Attributes().PutStr("link.type", "test")
		}
	}

	return traces
}

func TestIntegration_WriteTracesAndReadWithDuckDB(t *testing.T) {
	tc := setupIntegrationTest(t)
	defer tc.cleanup(t)

	tc.createTracesExporter(t)

	// Generate test traces
	traces := generateTestTraces(100)

	// Export traces
	ctx := context.Background()
	err := tc.tracesExporter.pushTracesData(ctx, traces)
	require.NoError(t, err)

	// Force flush by shutting down (which flushes pending batches)
	err = tc.tracesExporter.shutdown(ctx)
	require.NoError(t, err)
	tc.tracesExporter = nil

	// Verify parquet files were created
	files, err := filepath.Glob(filepath.Join(tc.tempDir, "main", "test_traces", "test_traces_*.parquet"))
	require.NoError(t, err)
	require.NotEmpty(t, files, "Expected parquet files to be created")

	t.Logf("Created %d trace parquet file(s): %v", len(files), files)

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Install and load ducklake extension
	_, err = db.ExecContext(ctx, "INSTALL ducklake")
	require.NoError(t, err, "DuckDB ducklake extension must be available for integration test")

	_, err = db.ExecContext(ctx, "LOAD ducklake")
	require.NoError(t, err)

	// Configure DuckDB to use our PostgreSQL metadata store
	pgDSN := fmt.Sprintf("host=%s port=%s dbname=ducklake_test user=test password=test",
		tc.pgHost, tc.pgPort)

	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		ATTACH 'ducklake:postgres:%s' AS otel;
	`, pgDSN))
	require.NoError(t, err)

	// Query traces via DuckDB ducklake extension
	query := "SELECT COUNT(*) FROM otel.main.test_traces"

	var count int
	err = db.QueryRowContext(ctx, query).Scan(&count)
	require.NoError(t, err)

	assert.Equal(t, 100, count, "Should be able to read 100 traces via DuckDB ducklake extension")

	// Verify schema by querying specific fields
	// First, let's see what columns are available
	describeQuery := "DESCRIBE SELECT * FROM otel.main.test_traces"
	rows, err := db.QueryContext(ctx, describeQuery)
	require.NoError(t, err)

	t.Log("Available columns in test_traces:")
	for rows.Next() {
		var colName, colType string
		var nullable, key, defaultVal, extra sql.NullString
		err = rows.Scan(&colName, &colType, &nullable, &key, &defaultVal, &extra)
		if err != nil {
			// Try simpler scan
			err = rows.Scan(&colName, &colType)
			if err != nil {
				continue
			}
		}
		t.Logf("  %s: %s", colName, colType)
	}
	rows.Close()

	// Query specific fields - using CAST to ensure string type
	query = `
		SELECT
			CAST(trace_id AS VARCHAR) as trace_id,
			CAST(span_id AS VARCHAR) as span_id,
			name,
			kind,
			start_time,
			end_time,
			duration_ns,
			status_code
		FROM otel.main.test_traces
		WHERE trace_id IS NOT NULL AND trace_id != ''
		LIMIT 1
	`

	var traceID, spanID, name string
	var kind int32
	var startTime, endTime time.Time
	var durationNs int64
	var statusCode int32

	err = db.QueryRowContext(ctx, query).Scan(&traceID, &spanID, &name, &kind, &startTime, &endTime, &durationNs, &statusCode)
	require.NoError(t, err)

	assert.NotEmpty(t, traceID, "trace_id should not be empty")
	assert.NotEmpty(t, spanID, "span_id should not be empty")
	assert.NotEmpty(t, name, "name should not be empty")

	t.Logf("Schema verification passed - sample trace: trace_id=%s, span_id=%s, name=%s", traceID, spanID, name)

	// Test bloom filter probe
	// Get a trace_id from the data
	query = "SELECT CAST(trace_id AS VARCHAR) FROM otel.main.test_traces WHERE trace_id IS NOT NULL AND trace_id != '' LIMIT 1"
	var testTraceID string
	err = db.QueryRowContext(ctx, query).Scan(&testTraceID)
	require.NoError(t, err)
	require.NotEmpty(t, testTraceID, "Should have a valid trace_id for bloom filter test")

	t.Logf("Testing bloom filter with trace_id: %s", testTraceID)

	// Run bloom filter probe query
	parquetPattern := filepath.Join(tc.tempDir, "main", "test_traces", "test_traces_*.parquet")
	bloomQuery := fmt.Sprintf(`
		SELECT
			file_name,
			row_group_id,
			bloom_filter_excludes
		FROM parquet_bloom_probe(
			'%s',
			'trace_id',
			'%s'
		)
	`, parquetPattern, testTraceID)

	rows, err = db.QueryContext(ctx, bloomQuery)
	require.NoError(t, err)
	defer rows.Close()

	// Verify at least one row has bloom_filter_excludes = false
	foundMatch := false
	rowCount := 0
	for rows.Next() {
		var fileName string
		var rowGroupID int
		var bloomFilterExcludes bool

		err = rows.Scan(&fileName, &rowGroupID, &bloomFilterExcludes)
		require.NoError(t, err)

		rowCount++
		if !bloomFilterExcludes {
			foundMatch = true
			t.Logf("Bloom filter match: file=%s, row_group=%d, excludes=%v", fileName, rowGroupID, bloomFilterExcludes)
		}
	}

	require.NoError(t, rows.Err())
	assert.Greater(t, rowCount, 0, "Should have at least one row group")
	assert.True(t, foundMatch, "At least one row group should have bloom_filter_excludes=false (indicating the trace_id might be present)")

	t.Logf("Bloom filter test passed: %d row groups checked, found match=%v", rowCount, foundMatch)
}

func TestIntegration_MultipleFlushes(t *testing.T) {
	tc := setupIntegrationTest(t)
	defer tc.cleanup(t)

	tc.createExporter(t)
	ctx := context.Background()

	// Send logs in multiple batches
	totalLogs := 0
	for i := 0; i < 3; i++ {
		logs := generateTestLogs(30)
		err := tc.exporter.pushLogsData(ctx, logs)
		require.NoError(t, err)

		// Wait for flush
		time.Sleep(2 * time.Second)
		totalLogs += 30
	}

	// Shutdown
	err := tc.exporter.shutdown(ctx)
	require.NoError(t, err)
	tc.exporter = nil

	// Verify total records
	dsn := fmt.Sprintf("host=%s port=%s dbname=ducklake_test user=test password=test sslmode=disable",
		tc.pgHost, tc.pgPort)

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()

	var totalRecords int64
	err = db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(record_count), 0)
		FROM public.ducklake_table_stats
	`).Scan(&totalRecords)
	require.NoError(t, err)

	assert.Equal(t, int64(totalLogs), totalRecords, "Should have all records across multiple flushes")

	// Verify snapshots were committed
	var snapshotCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.ducklake_snapshot").Scan(&snapshotCount)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, snapshotCount, 4, "Should have at least 4 snapshots") // 1 initial + 3 batches

	t.Logf("Multiple flushes test passed: %d total records, %d snapshots", totalRecords, snapshotCount)
}
