// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ducklakeexporter // import "github.com/hrl20/otel-ducklakeexporter"

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config defines configuration for the ducklake exporter.
type Config struct {
	TimeoutSettings           exporterhelper.TimeoutConfig `mapstructure:",squash"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`
	QueueSettings             configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`

	// PostgreSQL connection settings for DuckLake metadata store
	PostgreSQL PostgreSQLConfig `mapstructure:"postgresql"`

	// Parquet file settings
	Parquet ParquetConfig `mapstructure:"parquet"`

	// DuckLake metadata settings
	DuckLake DuckLakeConfig `mapstructure:"ducklake"`
}

// PostgreSQLConfig holds the configuration for the PostgreSQL metadata store.
type PostgreSQLConfig struct {
	// DSN is the PostgreSQL data source name. If provided, it takes precedence over individual fields.
	DSN configopaque.String `mapstructure:"dsn"`

	// Host is the PostgreSQL server host.
	Host string `mapstructure:"host"`
	// Port is the PostgreSQL server port.
	Port int `mapstructure:"port"`
	// Database is the PostgreSQL database name.
	Database string `mapstructure:"database"`
	// Username is the PostgreSQL username.
	Username string `mapstructure:"username"`
	// Password is the PostgreSQL password.
	Password configopaque.String `mapstructure:"password"`
	// SSLMode defines the SSL mode for the connection (disable, require, verify-ca, verify-full).
	SSLMode string `mapstructure:"ssl_mode"`

	// Schema is the PostgreSQL schema where metadata tables will be created.
	// This is different from the DuckLake schema name.
	Schema string `mapstructure:"schema"`

	// MaxOpenConns is the maximum number of open connections to the database.
	MaxOpenConns int `mapstructure:"max_open_conns"`
	// MaxIdleConns is the maximum number of idle connections in the pool.
	MaxIdleConns int `mapstructure:"max_idle_conns"`
	// ConnMaxLifetime is the maximum amount of time a connection may be reused.
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
}

// ParquetConfig holds the configuration for parquet file writing.
type ParquetConfig struct {
	// BatchSize is the number of log records to accumulate before writing a parquet file.
	BatchSize int `mapstructure:"batch_size"`
	// BatchTimeout is the maximum time to wait before writing a parquet file.
	BatchTimeout time.Duration `mapstructure:"batch_timeout"`

	// Compression is the compression codec to use (SNAPPY, GZIP, ZSTD, UNCOMPRESSED).
	Compression string `mapstructure:"compression"`

	// EnableBloomFilters enables bloom filters for trace_id and span_id columns.
	EnableBloomFilters bool `mapstructure:"enable_bloom_filters"`
}

// DuckLakeConfig holds the configuration for DuckLake storage and metadata.
type DuckLakeConfig struct {
	// OutputPath is the directory where parquet files will be written.
	// Can be either a local path (e.g., "/var/lib/ducklake/data") or an S3 URI (e.g., "s3://bucket/prefix").
	OutputPath string `mapstructure:"output_path"`

	// DataPath is the actual filesystem path to the parquet files (for metadata).
	// This may differ from OutputPath in containerized environments.
	// If not specified, defaults to OutputPath.
	// For S3 paths, this is ignored and OutputPath is used for metadata.
	DataPath string `mapstructure:"data_path"`

	// S3 settings (only used when OutputPath is an S3 URI)
	// Region is the AWS region for S3 (defaults to us-east-1 if not specified).
	S3Region string `mapstructure:"s3_region"`
	// S3Endpoint is the S3 endpoint URL for S3-compatible services (optional).
	S3Endpoint string `mapstructure:"s3_endpoint"`
	// S3ForcePathStyle forces path-style addressing (required for some S3-compatible services).
	S3ForcePathStyle bool `mapstructure:"s3_force_path_style"`
	// S3RoleARN is the AWS IAM role ARN to assume for S3 access (optional).
	S3RoleARN string `mapstructure:"s3_role_arn"`

	// LogsTableName is the name of the table for storing logs.
	LogsTableName string `mapstructure:"logs_table_name"`
	// TracesTableName is the name of the table for storing traces.
	TracesTableName string `mapstructure:"traces_table_name"`
}

// S3Config holds parsed S3 path information and configuration.
type S3Config struct {
	Bucket         string
	Prefix         string
	Region         string
	Endpoint       string
	ForcePathStyle bool
	RoleARN        string
}

const (
	defaultPostgreSQLHost     = "localhost"
	defaultPostgreSQLPort     = 5432
	defaultPostgreSQLDatabase = "ducklake"
	defaultPostgreSQLSchema   = "public" // PostgreSQL schema for metadata tables
	defaultPostgreSQLSSLMode  = "disable"
	defaultMaxOpenConns       = 25
	defaultMaxIdleConns       = 5
	defaultConnMaxLifetime    = 5 * time.Minute

	defaultOutputPath         = "/var/lib/ducklake/data"
	defaultBatchSize          = 10000
	defaultBatchTimeout       = 10 * time.Second // Reduced for testing
	defaultCompression        = "SNAPPY"
	defaultEnableBloomFilters = true

	defaultLogsTableName   = "otel_logs"
	defaultTracesTableName = "otel_traces"

	defaultS3Region = "us-east-1"

	// Hard-coded values (not configurable)
	schemaName = "main" // DuckLake schema name in PostgreSQL metadata
)

var (
	errConfigNoPostgreSQLConnection = errors.New("postgresql connection not configured: either dsn or host/port/database must be specified")
	errConfigInvalidBatchSize       = errors.New("batch_size must be greater than 0")
	errConfigInvalidBatchTimeout    = errors.New("batch_timeout must be greater than 0")
	errConfigInvalidOutputPath      = errors.New("output_path must be specified")
	errConfigInvalidSSLMode         = errors.New("ssl_mode must be one of: disable, require, verify-ca, verify-full")
	errConfigInvalidCompression     = errors.New("compression must be one of: SNAPPY, GZIP, ZSTD, UNCOMPRESSED")
	errConfigInvalidS3Path          = errors.New("invalid S3 path: must be in format s3://bucket/prefix")
)

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutSettings: exporterhelper.NewDefaultTimeoutConfig(),
		BackOffConfig:   configretry.NewDefaultBackOffConfig(),
		QueueSettings:   configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
		PostgreSQL: PostgreSQLConfig{
			Host:            defaultPostgreSQLHost,
			Port:            defaultPostgreSQLPort,
			Database:        defaultPostgreSQLDatabase,
			Schema:          defaultPostgreSQLSchema,
			SSLMode:         defaultPostgreSQLSSLMode,
			MaxOpenConns:    defaultMaxOpenConns,
			MaxIdleConns:    defaultMaxIdleConns,
			ConnMaxLifetime: defaultConnMaxLifetime,
		},
		Parquet: ParquetConfig{
			BatchSize:          defaultBatchSize,
			BatchTimeout:       defaultBatchTimeout,
			Compression:        defaultCompression,
			EnableBloomFilters: defaultEnableBloomFilters,
		},
		DuckLake: DuckLakeConfig{
			OutputPath:      defaultOutputPath,
			LogsTableName:   defaultLogsTableName,
			TracesTableName: defaultTracesTableName,
		},
	}
}

// Validate validates the ducklake exporter configuration.
func (cfg *Config) Validate() error {
	var err error

	// Validate PostgreSQL configuration
	if cfg.PostgreSQL.DSN == "" {
		if cfg.PostgreSQL.Host == "" || cfg.PostgreSQL.Database == "" {
			err = errors.Join(err, errConfigNoPostgreSQLConnection)
		}
	}

	// Validate SSL mode
	validSSLModes := map[string]bool{
		"disable":     true,
		"require":     true,
		"verify-ca":   true,
		"verify-full": true,
	}
	if !validSSLModes[cfg.PostgreSQL.SSLMode] {
		err = errors.Join(err, errConfigInvalidSSLMode)
	}

	// Validate DuckLake output path
	if cfg.DuckLake.OutputPath == "" {
		err = errors.Join(err, errConfigInvalidOutputPath)
	}

	// Validate output path
	if cfg.DuckLake.OutputPath != "" {
		// Check if it's an S3 path
		if isS3Path(cfg.DuckLake.OutputPath) {
			// Validate S3 path format
			if _, _, e := parseS3Path(cfg.DuckLake.OutputPath); e != nil {
				err = errors.Join(err, errConfigInvalidS3Path, e)
			}
			// For S3 paths, default region if not specified
			if cfg.DuckLake.S3Region == "" {
				cfg.DuckLake.S3Region = defaultS3Region
			}
			// DataPath should match OutputPath for S3
			cfg.DuckLake.DataPath = cfg.DuckLake.OutputPath
		} else {
			// For local paths, validate directory exists or can be created
			if info, e := os.Stat(cfg.DuckLake.OutputPath); e == nil {
				if !info.IsDir() {
					err = errors.Join(err, fmt.Errorf("output_path exists but is not a directory: %s", cfg.DuckLake.OutputPath))
				}
			}
			// Default DataPath to OutputPath if not specified
			if cfg.DuckLake.DataPath == "" {
				cfg.DuckLake.DataPath = cfg.DuckLake.OutputPath
			}
		}
	}

	// Validate parquet configuration
	if cfg.Parquet.BatchSize <= 0 {
		err = errors.Join(err, errConfigInvalidBatchSize)
	}

	if cfg.Parquet.BatchTimeout <= 0 {
		err = errors.Join(err, errConfigInvalidBatchTimeout)
	}

	// Validate compression
	validCompressions := map[string]bool{
		"SNAPPY":       true,
		"GZIP":         true,
		"ZSTD":         true,
		"UNCOMPRESSED": true,
	}
	if !validCompressions[cfg.Parquet.Compression] {
		err = errors.Join(err, errConfigInvalidCompression)
	}

	return err
}

// buildDSN builds a PostgreSQL DSN from the configuration.
func (cfg *Config) buildDSN() string {
	// If DSN is provided, use it directly
	if cfg.PostgreSQL.DSN != "" {
		return string(cfg.PostgreSQL.DSN)
	}

	// Build DSN from individual fields
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s sslmode=%s",
		cfg.PostgreSQL.Host,
		cfg.PostgreSQL.Port,
		cfg.PostgreSQL.Database,
		cfg.PostgreSQL.SSLMode,
	)

	if cfg.PostgreSQL.Username != "" {
		dsn += fmt.Sprintf(" user=%s", cfg.PostgreSQL.Username)
	}

	if cfg.PostgreSQL.Password != "" {
		dsn += fmt.Sprintf(" password=%s", string(cfg.PostgreSQL.Password))
	}

	return dsn
}

// isS3Path checks if a path is an S3 URI.
func isS3Path(path string) bool {
	return strings.HasPrefix(path, "s3://")
}

// parseS3Path parses an S3 URI into bucket and prefix.
// Example: s3://my-bucket/prefix/path -> ("my-bucket", "prefix/path", nil)
func parseS3Path(s3Path string) (bucket, prefix string, err error) {
	if !isS3Path(s3Path) {
		return "", "", fmt.Errorf("path does not start with s3://: %s", s3Path)
	}

	// Remove s3:// prefix
	path := strings.TrimPrefix(s3Path, "s3://")
	if path == "" {
		return "", "", fmt.Errorf("empty path after s3:// prefix")
	}

	// Split into bucket and prefix
	parts := strings.SplitN(path, "/", 2)
	bucket = parts[0]
	if bucket == "" {
		return "", "", fmt.Errorf("empty bucket name in S3 path: %s", s3Path)
	}

	if len(parts) > 1 {
		prefix = parts[1]
		// Trim trailing slashes from prefix
		prefix = strings.TrimSuffix(prefix, "/")
	}

	return bucket, prefix, nil
}

// GetS3Config returns an S3Config if the output path is an S3 URI, otherwise nil.
func (cfg *Config) GetS3Config() (*S3Config, error) {
	if !isS3Path(cfg.DuckLake.OutputPath) {
		return nil, nil
	}

	bucket, prefix, err := parseS3Path(cfg.DuckLake.OutputPath)
	if err != nil {
		return nil, err
	}

	region := cfg.DuckLake.S3Region
	if region == "" {
		region = defaultS3Region
	}

	return &S3Config{
		Bucket:         bucket,
		Prefix:         prefix,
		Region:         region,
		Endpoint:       cfg.DuckLake.S3Endpoint,
		ForcePathStyle: cfg.DuckLake.S3ForcePathStyle,
		RoleARN:        cfg.DuckLake.S3RoleARN,
	}, nil
}
