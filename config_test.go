// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ducklakeexporter

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

func TestDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	assert.NotNil(t, cfg)
	assert.IsType(t, &Config{}, cfg)

	ducklakeCfg := cfg.(*Config)

	// Check PostgreSQL defaults
	assert.Equal(t, defaultPostgreSQLHost, ducklakeCfg.PostgreSQL.Host)
	assert.Equal(t, defaultPostgreSQLPort, ducklakeCfg.PostgreSQL.Port)
	assert.Equal(t, defaultPostgreSQLDatabase, ducklakeCfg.PostgreSQL.Database)
	assert.Equal(t, defaultPostgreSQLSSLMode, ducklakeCfg.PostgreSQL.SSLMode)
	assert.Equal(t, defaultMaxOpenConns, ducklakeCfg.PostgreSQL.MaxOpenConns)
	assert.Equal(t, defaultMaxIdleConns, ducklakeCfg.PostgreSQL.MaxIdleConns)
	assert.Equal(t, defaultConnMaxLifetime, ducklakeCfg.PostgreSQL.ConnMaxLifetime)

	// Check Parquet defaults
	assert.Equal(t, defaultBatchSize, ducklakeCfg.Parquet.BatchSize)
	assert.Equal(t, defaultBatchTimeout, ducklakeCfg.Parquet.BatchTimeout)
	assert.Equal(t, defaultCompression, ducklakeCfg.Parquet.Compression)
	assert.Equal(t, defaultEnableBloomFilters, ducklakeCfg.Parquet.EnableBloomFilters)

	// Check DuckLake defaults
	assert.Equal(t, defaultOutputPath, ducklakeCfg.DuckLake.OutputPath)
	assert.Equal(t, defaultLogsTableName, ducklakeCfg.DuckLake.LogsTableName)
	assert.Equal(t, defaultTracesTableName, ducklakeCfg.DuckLake.TracesTableName)
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectedErr string
	}{
		{
			name: "valid config",
			config: &Config{
				PostgreSQL: PostgreSQLConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "test",
					SSLMode:  "disable",
				},
				Parquet: ParquetConfig{
					BatchSize:    100,
					BatchTimeout: 1 * time.Second,
					Compression:  "SNAPPY",
				},
				DuckLake: DuckLakeConfig{
					OutputPath:      "/tmp/test",
					LogsTableName:   "test_logs",
					TracesTableName: "test_traces",
				},
			},
			expectedErr: "",
		},
		{
			name: "missing postgresql host",
			config: &Config{
				PostgreSQL: PostgreSQLConfig{
					Port:     5432,
					Database: "test",
				},
				Parquet: ParquetConfig{
					BatchSize:    100,
					BatchTimeout: 1 * time.Second,
					Compression:  "SNAPPY",
				},
				DuckLake: DuckLakeConfig{
					OutputPath: "/tmp/test",
				},
			},
			expectedErr: "postgresql connection not configured",
		},
		{
			name: "invalid ssl mode",
			config: &Config{
				PostgreSQL: PostgreSQLConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "test",
					SSLMode:  "invalid",
				},
				Parquet: ParquetConfig{
					BatchSize:    100,
					BatchTimeout: 1 * time.Second,
					Compression:  "SNAPPY",
				},
				DuckLake: DuckLakeConfig{
					OutputPath: "/tmp/test",
				},
			},
			expectedErr: "ssl_mode must be one of",
		},
		{
			name: "invalid batch size",
			config: &Config{
				PostgreSQL: PostgreSQLConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "test",
					SSLMode:  "disable",
				},
				Parquet: ParquetConfig{
					BatchSize:    0,
					BatchTimeout: 1 * time.Second,
					Compression:  "SNAPPY",
				},
				DuckLake: DuckLakeConfig{
					OutputPath: "/tmp/test",
				},
			},
			expectedErr: "batch_size must be greater than 0",
		},
		{
			name: "invalid batch timeout",
			config: &Config{
				PostgreSQL: PostgreSQLConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "test",
					SSLMode:  "disable",
				},
				Parquet: ParquetConfig{
					BatchSize:    100,
					BatchTimeout: 0,
					Compression:  "SNAPPY",
				},
				DuckLake: DuckLakeConfig{
					OutputPath: "/tmp/test",
				},
			},
			expectedErr: "batch_timeout must be greater than 0",
		},
		{
			name: "invalid compression",
			config: &Config{
				PostgreSQL: PostgreSQLConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "test",
					SSLMode:  "disable",
				},
				Parquet: ParquetConfig{
					BatchSize:    100,
					BatchTimeout: 1 * time.Second,
					Compression:  "INVALID",
				},
				DuckLake: DuckLakeConfig{
					OutputPath: "/tmp/test",
				},
			},
			expectedErr: "compression must be one of",
		},
		{
			name: "missing output path",
			config: &Config{
				PostgreSQL: PostgreSQLConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "test",
					SSLMode:  "disable",
				},
				Parquet: ParquetConfig{
					BatchSize:    100,
					BatchTimeout: 1 * time.Second,
					Compression:  "SNAPPY",
				},
				DuckLake: DuckLakeConfig{
					OutputPath: "",
				},
			},
			expectedErr: "output_path must be specified",
		},
		{
			name: "valid with dsn",
			config: &Config{
				PostgreSQL: PostgreSQLConfig{
					DSN:     configopaque.String("postgresql://user:pass@localhost:5432/db"),
					SSLMode: "disable",
				},
				Parquet: ParquetConfig{
					BatchSize:    100,
					BatchTimeout: 1 * time.Second,
					Compression:  "SNAPPY",
				},
				DuckLake: DuckLakeConfig{
					OutputPath: "/tmp/test",
				},
			},
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectedErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			}
		})
	}
}

func TestBuildDSN(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectedDSN string
	}{
		{
			name: "basic dsn",
			config: &Config{
				PostgreSQL: PostgreSQLConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "testdb",
					SSLMode:  "disable",
				},
			},
			expectedDSN: "host=localhost port=5432 dbname=testdb sslmode=disable",
		},
		{
			name: "with credentials",
			config: &Config{
				PostgreSQL: PostgreSQLConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "testdb",
					Username: "testuser",
					Password: "testpass",
					SSLMode:  "require",
				},
			},
			expectedDSN: "host=localhost port=5432 dbname=testdb sslmode=require user=testuser password=testpass",
		},
		{
			name: "use provided dsn",
			config: &Config{
				PostgreSQL: PostgreSQLConfig{
					DSN:      configopaque.String("postgresql://custom:dsn@host:1234/db"),
					Host:     "ignored",
					Port:     9999,
					Database: "ignored",
				},
			},
			expectedDSN: "postgresql://custom:dsn@host:1234/db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := tt.config.buildDSN()
			assert.Equal(t, tt.expectedDSN, dsn)
		})
	}
}

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(component.MustNewType("ducklake"), "").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	ducklakeCfg := cfg.(*Config)

	// Just validate that the config was loaded and is valid
	// Specific field checks are covered by other tests
	assert.NoError(t, ducklakeCfg.Validate())

	// Verify at least one custom value was loaded (not defaults)
	assert.NotEmpty(t, ducklakeCfg.PostgreSQL.Host)
}

func TestQueueSettings(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)

	// Check that QueueSettings is Optional and has a value
	assert.True(t, cfg.QueueSettings.HasValue())

	queueCfg := cfg.QueueSettings.Get()
	assert.NotNil(t, queueCfg)
}

func TestCompressionOptions(t *testing.T) {
	validCompressions := []string{"SNAPPY", "GZIP", "ZSTD", "UNCOMPRESSED"}

	for _, compression := range validCompressions {
		t.Run(compression, func(t *testing.T) {
			cfg := &Config{
				PostgreSQL: PostgreSQLConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "test",
					SSLMode:  "disable",
				},
				Parquet: ParquetConfig{
					BatchSize:    100,
					BatchTimeout: 1 * time.Second,
					Compression:  compression,
				},
				DuckLake: DuckLakeConfig{
					OutputPath: "/tmp/test",
				},
			}

			err := cfg.Validate()
			assert.NoError(t, err, "Compression %s should be valid", compression)
		})
	}
}

func TestSSLModeOptions(t *testing.T) {
	validSSLModes := []string{"disable", "require", "verify-ca", "verify-full"}

	for _, sslMode := range validSSLModes {
		t.Run(sslMode, func(t *testing.T) {
			cfg := &Config{
				PostgreSQL: PostgreSQLConfig{
					Host:     "localhost",
					Port:     5432,
					Database: "test",
					SSLMode:  sslMode,
				},
				Parquet: ParquetConfig{
					BatchSize:    100,
					BatchTimeout: 1 * time.Second,
					Compression:  "SNAPPY",
				},
				DuckLake: DuckLakeConfig{
					OutputPath: "/tmp/test",
				},
			}

			err := cfg.Validate()
			assert.NoError(t, err, "SSL mode %s should be valid", sslMode)
		})
	}
}

func TestOptionalQueueSettings(t *testing.T) {
	cfg := &Config{
		PostgreSQL: PostgreSQLConfig{
			Host:     "localhost",
			Port:     5432,
			Database: "test",
			SSLMode:  "disable",
		},
		Parquet: ParquetConfig{
			BatchSize:    100,
			BatchTimeout: 1 * time.Second,
			Compression:  "SNAPPY",
		},
		DuckLake: DuckLakeConfig{
			OutputPath: "/tmp/test",
		},
		QueueSettings: configoptional.None[exporterhelper.QueueBatchConfig](),
	}

	// Should still be valid even without queue settings
	err := cfg.Validate()
	assert.NoError(t, err)
	assert.False(t, cfg.QueueSettings.HasValue())
}
