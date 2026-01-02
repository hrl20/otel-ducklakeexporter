// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ducklakeexporter // import "github.com/hrl20/otel-ducklakeexporter"

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/hrl20/otel-ducklakeexporter/internal/parquet"
	"github.com/hrl20/otel-ducklakeexporter/internal/postgres"
)

// tracesExporter implements the traces exporter for DuckLake.
type tracesExporter struct {
	config *Config
	logger *zap.Logger

	// PostgreSQL client for metadata storage
	pgClient *postgres.Client

	// Metadata writer for registering parquet files
	metadataWriter *postgres.MetadataWriter

	// Parquet batch writer
	batchWriter *parquet.TraceBatchWriter

	// S3 uploader (optional)
	s3Uploader *S3Uploader
}

// newTracesExporter creates a new DuckLake traces exporter.
func newTracesExporter(logger *zap.Logger, cfg *Config) *tracesExporter {
	return &tracesExporter{
		config: cfg,
		logger: logger,
	}
}

// start initializes the exporter.
func (e *tracesExporter) start(ctx context.Context, _ component.Host) error {
	e.logger.Info("starting ducklake traces exporter",
		zap.String("output_path", e.config.DuckLake.OutputPath),
		zap.String("postgresql_host", e.config.PostgreSQL.Host))

	// Initialize PostgreSQL client
	pgConfig := postgres.Config{
		DSN:             e.config.buildDSN(),
		SchemaName:      e.config.PostgreSQL.Schema,
		MaxOpenConns:    e.config.PostgreSQL.MaxOpenConns,
		MaxIdleConns:    e.config.PostgreSQL.MaxIdleConns,
		ConnMaxLifetime: e.config.PostgreSQL.ConnMaxLifetime,
	}

	client, err := postgres.NewClient(pgConfig, e.logger)
	if err != nil {
		return fmt.Errorf("failed to create postgresql client: %w", err)
	}
	e.pgClient = client

	// Verify PostgreSQL connection
	if err := e.pgClient.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping postgresql: %w", err)
	}

	e.logger.Info("successfully connected to postgresql")

	// Check if output path is S3
	s3Config, err := e.config.GetS3Config()
	if err != nil {
		return fmt.Errorf("failed to parse S3 config: %w", err)
	}

	// Determine the data path for metadata
	dataPath := e.config.DuckLake.DataPath
	if s3Config != nil {
		// S3 mode: use S3 URI for metadata
		dataPath = e.config.DuckLake.OutputPath
		e.logger.Info("S3 output path detected", zap.String("s3_path", dataPath))
	} else {
		// Local mode: ensure output directory exists
		if err := os.MkdirAll(e.config.DuckLake.OutputPath, 0755); err != nil {
			return fmt.Errorf("failed to create output directory: %w", err)
		}
	}

	// Create DuckLake metadata schema (always enabled)
	if err := e.pgClient.CreateSchema(ctx, dataPath); err != nil {
		return fmt.Errorf("failed to create ducklake schema: %w", err)
	}

	// Initialize metadata writer
	e.metadataWriter = postgres.NewMetadataWriter(
		e.pgClient,
		schemaName,
		e.config.DuckLake.TracesTableName,
		"traces",
		e.config.DuckLake.OutputPath,
		dataPath,
		e.logger,
	)

	// Initialize S3 uploader if in S3 mode
	if s3Config != nil {
		s3Uploader, err := NewS3Uploader(ctx, s3Config, e.logger)
		if err != nil {
			return fmt.Errorf("failed to create S3 uploader: %w", err)
		}
		e.s3Uploader = s3Uploader
		e.logger.Info("S3 uploader initialized for traces exporter",
			zap.String("bucket", s3Config.Bucket),
			zap.String("prefix", s3Config.Prefix),
			zap.String("region", s3Config.Region))
	}

	// Initialize parquet batch writer
	parquetConfig := &parquet.Config{
		OutputPath:         e.config.DuckLake.OutputPath,
		SchemaName:         schemaName,
		TableName:          e.config.DuckLake.TracesTableName,
		BatchSize:          e.config.Parquet.BatchSize,
		BatchTimeout:       e.config.Parquet.BatchTimeout,
		FilePrefix:         e.config.DuckLake.TracesTableName,
		Compression:        e.config.Parquet.Compression,
		EnableBloomFilters: e.config.Parquet.EnableBloomFilters,
	}

	// Create callback for file written events
	onFileWritten := func(stats parquet.FileStats) error {
		if e.s3Uploader != nil {
			// S3 mode: upload parquet data from memory
			// Compute S3 key from file path
			// Example: /Users/.../main/otel_traces/file.parquet -> main/otel_traces/file.parquet
			relPath, err := filepath.Rel(e.config.DuckLake.OutputPath, stats.FilePath)
			if err != nil {
				e.logger.Error("failed to compute S3 key",
					zap.Error(err),
					zap.String("file_path", stats.FilePath),
					zap.String("output_path", e.config.DuckLake.OutputPath))
				return fmt.Errorf("failed to compute S3 key: %w", err)
			}

			// Upload bytes directly to S3
			s3URI, err := e.s3Uploader.UploadBytes(ctx, stats.Data, relPath)
			if err != nil {
				e.logger.Error("failed to upload to S3", zap.Error(err), zap.String("s3_key", relPath))
				return fmt.Errorf("failed to upload to S3: %w", err)
			}

			// Update file path to S3 URI for metadata
			stats.FilePath = s3URI
		} else {
			// Local mode: write parquet data to disk
			if err := os.WriteFile(stats.FilePath, stats.Data, 0644); err != nil {
				e.logger.Error("failed to write file to disk",
					zap.Error(err),
					zap.String("file_path", stats.FilePath))
				return fmt.Errorf("failed to write file to disk: %w", err)
			}
		}

		// Clear data from stats before registering (save memory)
		stats.Data = nil
		return e.metadataWriter.RegisterParquetFile(ctx, stats)
	}

	batchWriter, err := parquet.NewTraceBatchWriter(parquetConfig, e.logger, onFileWritten)
	if err != nil {
		return fmt.Errorf("failed to create batch writer: %w", err)
	}
	e.batchWriter = batchWriter

	// Start the batch writer's background timer
	e.batchWriter.Start()

	e.logger.Info("ducklake traces exporter started successfully")

	return nil
}

// shutdown stops the exporter and flushes any pending data.
func (e *tracesExporter) shutdown(ctx context.Context) error {
	e.logger.Info("shutting down ducklake traces exporter")

	var errs []error

	// Shutdown batch writer (flushes pending data)
	if e.batchWriter != nil {
		if err := e.batchWriter.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to shutdown batch writer: %w", err))
		}
	}

	// Close PostgreSQL connection
	if e.pgClient != nil {
		if err := e.pgClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close postgresql client: %w", err))
		}
	}

	if len(errs) > 0 {
		e.logger.Error("errors during shutdown", zap.Errors("errors", errs))
		return fmt.Errorf("shutdown errors: %v", errs)
	}

	e.logger.Info("ducklake traces exporter shutdown complete")

	return nil
}

// pushTracesData processes and exports trace data.
func (e *tracesExporter) pushTracesData(ctx context.Context, td ptrace.Traces) error {
	// Iterate through resource spans
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpans := td.ResourceSpans().At(i)
		resource := resourceSpans.Resource()
		resourceAttrs := resource.Attributes()

		// Iterate through scope spans
		for j := 0; j < resourceSpans.ScopeSpans().Len(); j++ {
			scopeSpans := resourceSpans.ScopeSpans().At(j)
			scope := scopeSpans.Scope()
			scopeName := scope.Name()
			scopeVersion := scope.Version()

			// Iterate through spans
			for k := 0; k < scopeSpans.Spans().Len(); k++ {
				span := scopeSpans.Spans().At(k)

				// Convert to OTLPSpan
				otlpSpan, err := parquet.FromSpan(span, resourceAttrs, scopeName, scopeVersion)
				if err != nil {
					e.logger.Error("failed to convert span", zap.Error(err))
					continue
				}

				// Add to batch writer
				if err := e.batchWriter.AddSpan(otlpSpan); err != nil {
					return fmt.Errorf("failed to add span to batch: %w", err)
				}
			}
		}
	}

	return nil
}
