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
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"go.uber.org/zap"
)

// Config holds the configuration for the parquet batch writer.
type Config struct {
	OutputPath         string
	SchemaName         string
	TableName          string
	BatchSize          int
	BatchTimeout       time.Duration
	FilePrefix         string
	Compression        string
	EnableBloomFilters bool
}

// FileStats holds statistics about a written parquet file.
type FileStats struct {
	FilePath      string
	RecordCount   int64
	FileSizeBytes int64
	FooterSize    int64
	ColumnStats   []ColumnStats
	Data          []byte // Parquet data in memory (for S3 uploads or writing to disk)
}

// ColumnStats holds statistics for a parquet column.
type ColumnStats struct {
	ColumnName    string
	MinValue      string
	MaxValue      string
	NullCount     int64
	DistinctCount int64
	SizeBytes     int64
}

// BatchWriter accumulates log records and writes them to parquet files.
type BatchWriter struct {
	config *Config
	logger *zap.Logger

	// Batching state
	batch      []*OTLPLog
	batchMutex sync.Mutex
	lastFlush  time.Time

	// Timer for time-based flushing
	flushTimer *time.Timer
	timerMutex sync.Mutex

	// Callback for file written events
	onFileWritten func(stats FileStats) error

	// Shutdown coordination
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
}

// NewBatchWriter creates a new parquet batch writer.
func NewBatchWriter(config *Config, logger *zap.Logger, onFileWritten func(FileStats) error) (*BatchWriter, error) {
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

	return &BatchWriter{
		config:        config,
		logger:        logger,
		batch:         make([]*OTLPLog, 0, config.BatchSize),
		lastFlush:     time.Now(),
		onFileWritten: onFileWritten,
		ctx:           ctx,
		cancelFunc:    cancel,
	}, nil
}

// Start starts the batch writer's background timer.
func (bw *BatchWriter) Start() {
	bw.wg.Add(1)
	go bw.flushLoop()
}

// AddLog adds a log record to the batch.
func (bw *BatchWriter) AddLog(log *OTLPLog) error {
	bw.batchMutex.Lock()
	defer bw.batchMutex.Unlock()

	bw.batch = append(bw.batch, log)

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

// Flush flushes any pending logs to a parquet file.
func (bw *BatchWriter) Flush(ctx context.Context) error {
	bw.batchMutex.Lock()
	defer bw.batchMutex.Unlock()

	return bw.flushLocked()
}

// flushLoop runs a background timer that flushes batches periodically.
func (bw *BatchWriter) flushLoop() {
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
func (bw *BatchWriter) flushLocked() error {
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

// writeParquetFile writes a batch of logs to parquet format in memory and returns the bytes.
// The caller is responsible for writing the bytes to a file or uploading to S3.
func (bw *BatchWriter) writeParquetFile(filename string, logs []*OTLPLog) ([]byte, FileStats, error) {
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

	// Note: Bloom filters in parquet-go require specific configuration
	// They are automatically enabled for dictionary-encoded string columns
	// The EnableBloomFilters flag is noted but not actively configured here
	// This can be enhanced in future versions with proper bloom filter configuration

	// Create parquet writer to memory buffer
	writer := parquet.NewGenericWriter[OTLPLog](buf, writerOptions...)

	// Compute column statistics from the batch before writing
	columnStats := computeColumnStatsFromBatch(logs)

	// Write all logs
	for _, log := range logs {
		_, err := writer.Write([]OTLPLog{*log})
		if err != nil {
			return nil, FileStats{}, fmt.Errorf("failed to write log: %w", err)
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
		RecordCount:   int64(len(logs)),
		FileSizeBytes: int64(len(data)),
		FooterSize:    footerSize,
		ColumnStats:   columnStats,
	}

	return data, stats, nil
}

// readParquetFooterSizeFromBytes reads the footer size from parquet bytes in memory.
// The Parquet format ends with: [Footer][4-byte footer length][4-byte magic "PAR1"]
// The footer length is the size of the serialized Thrift FileMetaData.
func readParquetFooterSizeFromBytes(data []byte) (int64, error) {
	dataSize := len(data)

	// Minimum valid parquet file: magic(4) + footer(1+) + footer_len(4) + magic(4) = 13+ bytes
	if dataSize < 13 {
		return 0, fmt.Errorf("data too small to be a valid parquet file")
	}

	// Read the footer length from position (length - 8): 4 bytes before the trailing magic
	footerLengthBytes := data[dataSize-8 : dataSize-4]

	// Convert little-endian bytes to int32
	footerLength := int64(footerLengthBytes[0]) |
		int64(footerLengthBytes[1])<<8 |
		int64(footerLengthBytes[2])<<16 |
		int64(footerLengthBytes[3])<<24

	// Validate footer length is reasonable
	if footerLength < 0 || footerLength > int64(dataSize-12) {
		return 0, fmt.Errorf("invalid footer length: %d (data size: %d)", footerLength, dataSize)
	}

	return footerLength, nil
}

// isS3Path checks if a path is an S3 URI.
func isS3Path(path string) bool {
	return strings.HasPrefix(path, "s3://")
}

// computeColumnStatsFromBatch computes column statistics from a batch of logs.
func computeColumnStatsFromBatch(logs []*OTLPLog) []ColumnStats {
	recordCount := int64(len(logs))

	// Initialize stats map by column name
	statsMap := make(map[string]*ColumnStats)
	columnNames := GetColumnNames()
	for _, name := range columnNames {
		statsMap[name] = &ColumnStats{
			ColumnName:    name,
			MinValue:      "",
			MaxValue:      "",
			NullCount:     0,
			DistinctCount: recordCount, // value_count = total values (not distinct)
			SizeBytes:     0,
		}
	}

	// Iterate through all logs and update statistics
	// Skip min/max for JSON columns (resource_attributes, log_attributes) as they don't make sense
	for _, log := range logs {
		updateColumnStats(statsMap, "timestamp", log.Timestamp.Format("2006-01-02 15:04:05.000000-07"))
		updateColumnStats(statsMap, "trace_id", log.TraceID)
		updateColumnStats(statsMap, "span_id", log.SpanID)
		updateColumnStats(statsMap, "severity_number", fmt.Sprintf("%d", log.SeverityNumber))
		updateColumnStats(statsMap, "severity_text", log.SeverityText)
		updateColumnStats(statsMap, "body", log.Body)
		updateColumnStats(statsMap, "scope_name", log.ScopeName)
		updateColumnStats(statsMap, "scope_version", log.ScopeVersion)
		// Skip resource_attributes and log_attributes for min/max (JSON doesn't make sense)
		updateNullCountOnly(statsMap, "resource_attributes", log.ResourceAttributes)
		updateNullCountOnly(statsMap, "log_attributes", log.LogAttributes)
		updateColumnStats(statsMap, "flags", fmt.Sprintf("%d", log.Flags))
		updateColumnStats(statsMap, "observed_timestamp", log.ObservedTime.Format("2006-01-02 15:04:05.000000-07"))
	}

	// Convert map to slice
	stats := make([]ColumnStats, 0, len(statsMap))
	for _, stat := range statsMap {
		stats = append(stats, *stat)
	}

	return stats
}

// updateColumnStats updates min/max for a column based on a value.
func updateColumnStats(statsMap map[string]*ColumnStats, columnName, value string) {
	stat := statsMap[columnName]
	if stat == nil {
		return
	}

	// Track byte size
	stat.SizeBytes += int64(len(value))

	// Check for empty/null values
	if value == "" {
		stat.NullCount++
		return
	}

	// Update min
	if stat.MinValue == "" || value < stat.MinValue {
		stat.MinValue = value
	}

	// Update max
	if stat.MaxValue == "" || value > stat.MaxValue {
		stat.MaxValue = value
	}
}

// updateNullCountOnly updates only the null count for a column (used for JSON columns where min/max doesn't make sense).
func updateNullCountOnly(statsMap map[string]*ColumnStats, columnName, value string) {
	stat := statsMap[columnName]
	if stat == nil {
		return
	}

	// Track byte size
	stat.SizeBytes += int64(len(value))

	// Check for empty/null values
	if value == "" || value == "{}" {
		stat.NullCount++
	}
}

// Shutdown stops the batch writer and flushes any pending data.
func (bw *BatchWriter) Shutdown(ctx context.Context) error {
	bw.logger.Info("shutting down batch writer")

	// Stop the flush loop
	bw.cancelFunc()
	bw.wg.Wait()

	// Flush any remaining data
	if err := bw.Flush(ctx); err != nil {
		return fmt.Errorf("failed to flush on shutdown: %w", err)
	}

	return nil
}
