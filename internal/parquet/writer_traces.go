// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package parquet // import "github.com/hrl20/otel-ducklakeexporter/internal/parquet"

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"go.uber.org/zap"
)

// TraceBatchWriter accumulates span records and writes them to parquet files.
type TraceBatchWriter struct {
	config *Config
	logger *zap.Logger

	// Batching state
	batch      []*OTLPSpan
	batchMutex sync.Mutex
	lastFlush  time.Time

	// Callback for file written events
	onFileWritten func(stats FileStats) error

	// Shutdown coordination
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

// NewTraceBatchWriter creates a new parquet batch writer for traces.
func NewTraceBatchWriter(config *Config, logger *zap.Logger, onFileWritten func(FileStats) error) (*TraceBatchWriter, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create subdirectory structure: OutputPath/SchemaName/TableName/
	// Use path.Join for S3 (forward slashes), filepath.Join for local (OS-specific)
	var fullOutputPath string
	if isS3Path(config.OutputPath) {
		fullOutputPath = path.Join(config.OutputPath, config.SchemaName, config.TableName)
	} else {
		fullOutputPath = filepath.Join(config.OutputPath, config.SchemaName, config.TableName)
		if err := os.MkdirAll(fullOutputPath, 0755); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create output directory %s: %w", fullOutputPath, err)
		}
	}

	// Update config to use the full path
	config.OutputPath = fullOutputPath

	return &TraceBatchWriter{
		config:        config,
		logger:        logger,
		batch:         make([]*OTLPSpan, 0, config.BatchSize),
		lastFlush:     time.Now(),
		onFileWritten: onFileWritten,
		ctx:           ctx,
		cancelFunc:    cancel,
	}, nil
}

// Start starts the batch writer's background timer.
func (bw *TraceBatchWriter) Start() {
	bw.wg.Add(1)
	go bw.flushLoop()
}

// AddSpan adds a span record to the batch.
func (bw *TraceBatchWriter) AddSpan(span *OTLPSpan) error {
	bw.batchMutex.Lock()
	defer bw.batchMutex.Unlock()

	bw.batch = append(bw.batch, span)

	// Check if we've reached the batch size threshold
	if len(bw.batch) >= bw.config.BatchSize {
		bw.logger.Debug("batch size threshold reached, flushing",
			zap.Int("batch_size", len(bw.batch)),
			zap.Int("threshold", bw.config.BatchSize))

		if err := bw.flushLocked(); err != nil {
			return fmt.Errorf("failed to flush batch: %w", err)
		}
	}

	return nil
}

// Flush flushes any pending spans to a parquet file.
func (bw *TraceBatchWriter) Flush(ctx context.Context) error {
	bw.batchMutex.Lock()
	defer bw.batchMutex.Unlock()

	return bw.flushLocked()
}

// flushLoop runs a background timer that flushes batches periodically.
func (bw *TraceBatchWriter) flushLoop() {
	defer bw.wg.Done()

	ticker := time.NewTicker(bw.config.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-bw.ctx.Done():
			return
		case <-ticker.C:
			bw.batchMutex.Lock()
			if len(bw.batch) > 0 {
				bw.logger.Debug("batch timeout reached, flushing",
					zap.Int("batch_size", len(bw.batch)),
					zap.Duration("timeout", bw.config.BatchTimeout))

				if err := bw.flushLocked(); err != nil {
					bw.logger.Error("failed to flush batch on timeout", zap.Error(err))
				}
			}
			bw.batchMutex.Unlock()
		}
	}
}

// flushLocked flushes the current batch to a parquet file.
// Must be called with batchMutex held.
func (bw *TraceBatchWriter) flushLocked() error {
	if len(bw.batch) == 0 {
		return nil
	}

	// Generate filename: <prefix>_<timestamp>_<uuid>.parquet
	timestamp := time.Now().UTC().Format("20060102_150405")
	shortUUID := uuid.New().String()[:8]
	filename := fmt.Sprintf("%s_%s_%s.parquet", bw.config.FilePrefix, timestamp, shortUUID)

	// Use path.Join for S3, filepath.Join for local
	var filePath string
	if isS3Path(bw.config.OutputPath) {
		filePath = path.Join(bw.config.OutputPath, filename)
	} else {
		filePath = filepath.Join(bw.config.OutputPath, filename)
	}

	bw.logger.Info("writing parquet file",
		zap.String("file", filename),
		zap.Int("records", len(bw.batch)))

	// Write parquet data to memory
	data, stats, err := bw.writeParquetFile(filename, bw.batch)
	if err != nil {
		return fmt.Errorf("failed to write parquet file: %w", err)
	}

	// Add data to stats and set file path
	stats.Data = data
	stats.FilePath = filePath

	// Call the file written callback (callback handles writing to disk or uploading to S3)
	if bw.onFileWritten != nil {
		if err := bw.onFileWritten(stats); err != nil {
			// Log error but don't fail the flush
			bw.logger.Error("failed to process parquet file", zap.Error(err))
		}
	}

	// Clear the batch
	bw.batch = bw.batch[:0]
	bw.lastFlush = time.Now()

	bw.logger.Info("successfully wrote parquet file",
		zap.String("file", filename),
		zap.Int64("records", stats.RecordCount),
		zap.Int64("size_bytes", stats.FileSizeBytes))

	return nil
}

// writeParquetFile writes a batch of spans to parquet format in memory and returns the bytes.
// The caller is responsible for writing the bytes to a file or uploading to S3.
func (bw *TraceBatchWriter) writeParquetFile(filename string, spans []*OTLPSpan) ([]byte, FileStats, error) {
	// Create in-memory buffer
	buf := &bytes.Buffer{}

	// Determine compression codec
	var codec compress.Codec
	switch bw.config.Compression {
	case "SNAPPY":
		codec = &parquet.Snappy
	case "GZIP":
		codec = &parquet.Gzip
	case "ZSTD":
		codec = &parquet.Zstd
	case "UNCOMPRESSED":
		codec = &parquet.Uncompressed
	default:
		codec = &parquet.Snappy
	}

	// Build writer options
	writerOptions := []parquet.WriterOption{
		parquet.Compression(codec),
	}

	// Add bloom filters for trace_id and span_id columns if enabled
	// Using 10 bits per value provides ~1% false positive rate
	if bw.config.EnableBloomFilters {
		writerOptions = append(writerOptions, parquet.BloomFilters(
			parquet.SplitBlockFilter(10, "trace_id"),
			parquet.SplitBlockFilter(10, "span_id"),
		))
		bw.logger.Debug("enabled bloom filters for trace_id and span_id columns")
	}

	// Create parquet writer to memory buffer
	writer := parquet.NewGenericWriter[OTLPSpan](buf, writerOptions...)

	// Compute column statistics from the batch before writing
	columnStats := computeSpanColumnStatsFromBatch(spans)

	// Write all spans
	for _, span := range spans {
		_, err := writer.Write([]OTLPSpan{*span})
		if err != nil {
			return nil, FileStats{}, fmt.Errorf("failed to write span: %w", err)
		}
	}

	// Close writer to flush data
	if err := writer.Close(); err != nil {
		return nil, FileStats{}, fmt.Errorf("failed to close writer: %w", err)
	}

	// Get the bytes
	data := buf.Bytes()

	// Read footer size from parquet bytes
	footerSize, err := readParquetFooterSizeFromBytes(data)
	if err != nil {
		bw.logger.Warn("failed to read parquet footer size", zap.Error(err))
		footerSize = 0
	}

	stats := FileStats{
		FilePath:      filename,
		RecordCount:   int64(len(spans)),
		FileSizeBytes: int64(len(data)),
		FooterSize:    footerSize,
		ColumnStats:   columnStats,
	}

	return data, stats, nil
}

// computeSpanColumnStatsFromBatch computes column statistics from a batch of spans.
func computeSpanColumnStatsFromBatch(spans []*OTLPSpan) []ColumnStats {
	recordCount := int64(len(spans))

	// Initialize stats map by column name
	statsMap := make(map[string]*ColumnStats)
	columnNames := GetSpanColumnNames()
	for _, name := range columnNames {
		statsMap[name] = &ColumnStats{
			ColumnName:    name,
			MinValue:      "",
			MaxValue:      "",
			NullCount:     0,
			DistinctCount: recordCount,
			SizeBytes:     0,
		}
	}

	// Iterate through all spans and update statistics
	for _, span := range spans {
		updateColumnStats(statsMap, "trace_id", span.TraceID)
		updateColumnStats(statsMap, "span_id", span.SpanID)
		updateColumnStats(statsMap, "parent_span_id", span.ParentSpanID)
		updateColumnStats(statsMap, "name", span.Name)
		updateColumnStats(statsMap, "kind", fmt.Sprintf("%d", span.Kind))
		updateColumnStats(statsMap, "start_time", span.StartTime.Format("2006-01-02 15:04:05.000000-07"))
		updateColumnStats(statsMap, "end_time", span.EndTime.Format("2006-01-02 15:04:05.000000-07"))
		updateColumnStats(statsMap, "duration_ns", fmt.Sprintf("%d", span.DurationNs))
		updateColumnStats(statsMap, "status_code", fmt.Sprintf("%d", span.StatusCode))
		updateColumnStats(statsMap, "status_message", span.StatusMessage)
		updateColumnStats(statsMap, "scope_name", span.ScopeName)
		updateColumnStats(statsMap, "scope_version", span.ScopeVersion)
		// Skip JSON columns for min/max
		updateNullCountOnly(statsMap, "resource_attributes", span.ResourceAttributes)
		updateNullCountOnly(statsMap, "span_attributes", span.SpanAttributes)
		updateNullCountOnly(statsMap, "events", span.Events)
		updateNullCountOnly(statsMap, "links", span.Links)
		updateColumnStats(statsMap, "dropped_attributes_count", fmt.Sprintf("%d", span.DroppedAttributesCount))
		updateColumnStats(statsMap, "dropped_events_count", fmt.Sprintf("%d", span.DroppedEventsCount))
		updateColumnStats(statsMap, "dropped_links_count", fmt.Sprintf("%d", span.DroppedLinksCount))
		updateColumnStats(statsMap, "trace_state", span.TraceState)
		updateColumnStats(statsMap, "flags", fmt.Sprintf("%d", span.Flags))
	}

	// Convert map to slice
	stats := make([]ColumnStats, 0, len(statsMap))
	for _, stat := range statsMap {
		stats = append(stats, *stat)
	}

	return stats
}

// Shutdown stops the batch writer and flushes any pending data.
func (bw *TraceBatchWriter) Shutdown(ctx context.Context) error {
	bw.logger.Info("shutting down trace batch writer")

	// Stop the flush loop
	bw.cancelFunc()
	bw.wg.Wait()

	// Flush any remaining data
	if err := bw.Flush(ctx); err != nil {
		return fmt.Errorf("failed to flush on shutdown: %w", err)
	}

	return nil
}
