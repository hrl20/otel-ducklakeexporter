// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package postgres // import "github.com/hrl20/otel-ducklakeexporter/internal/postgres"

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"go.uber.org/zap"
)

// Client wraps a PostgreSQL database connection.
type Client struct {
	db         *sql.DB
	schemaName string
	logger     *zap.Logger
}

// Config holds the configuration for the PostgreSQL client.
type Config struct {
	DSN             string
	SchemaName      string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// NewClient creates a new PostgreSQL client.
func NewClient(cfg Config, logger *zap.Logger) (*Client, error) {
	db, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	client := &Client{
		db:         db,
		schemaName: cfg.SchemaName,
		logger:     logger,
	}

	return client, nil
}

// Ping verifies the connection to the database is alive.
func (c *Client) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// Close closes the database connection.
func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// DB returns the underlying database connection.
func (c *Client) DB() *sql.DB {
	return c.db
}

// SchemaName returns the configured schema name.
func (c *Client) SchemaName() string {
	return c.schemaName
}
