// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ducklakeexporter // import "github.com/hrl20/otel-ducklakeexporter"

import (
	"bytes"
	"context"
	"fmt"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"go.uber.org/zap"
)

// S3Uploader handles uploading parquet files to S3.
type S3Uploader struct {
	client   *s3.Client
	uploader *manager.Uploader
	config   *S3Config
	logger   *zap.Logger
}

// NewS3Uploader creates a new S3 uploader.
func NewS3Uploader(ctx context.Context, cfg *S3Config, logger *zap.Logger) (*S3Uploader, error) {
	if cfg == nil {
		return nil, nil
	}

	// Load AWS config with region
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// If RoleARN is specified, assume the role
	if cfg.RoleARN != "" {
		stsClient := sts.NewFromConfig(awsCfg)
		creds := stscreds.NewAssumeRoleProvider(stsClient, cfg.RoleARN)
		awsCfg.Credentials = aws.NewCredentialsCache(creds)
	}

	// Create S3 client with custom options
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		// Custom endpoint for S3-compatible services
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		// Force path-style addressing if configured
		o.UsePathStyle = cfg.ForcePathStyle
	})

	// Create S3 manager uploader for automatic multipart upload handling
	// Automatically uses PutObject for small files, multipart for large files
	uploader := manager.NewUploader(s3Client)

	logger.Info("initialized S3 uploader",
		zap.String("region", cfg.Region),
		zap.String("bucket", cfg.Bucket),
		zap.String("prefix", cfg.Prefix))

	return &S3Uploader{
		client:   s3Client,
		uploader: uploader,
		config:   cfg,
		logger:   logger,
	}, nil
}

// UploadBytes uploads data from memory to S3 with the given S3 key.
// Uses S3 Manager which automatically handles:
// - Single PutObject for small files (< 5MB)
// - Multipart upload for large files (>= 5MB) with parallel part uploads
// The key should be the relative path within the S3 bucket (e.g., "main/otel_logs/file.parquet").
// Returns the S3 URI (s3://bucket/key) of the uploaded file.
func (u *S3Uploader) UploadBytes(ctx context.Context, data []byte, s3Key string) (string, error) {
	// Build full S3 key with prefix if configured
	var fullKey string
	if u.config.Prefix != "" {
		// Use path.Join for clean URL paths
		fullKey = path.Join(u.config.Prefix, s3Key)
	} else {
		fullKey = s3Key
	}

	// Upload to S3 using manager (handles multipart automatically)
	u.logger.Debug("uploading data to S3",
		zap.String("bucket", u.config.Bucket),
		zap.String("key", fullKey),
		zap.Int("size_bytes", len(data)))

	_, err := u.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(u.config.Bucket),
		Key:    aws.String(fullKey),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return "", fmt.Errorf("failed to upload data to S3: %w", err)
	}

	// Build S3 URI using path.Join for clean URL construction
	s3URI := fmt.Sprintf("s3://%s/%s", u.config.Bucket, fullKey)

	u.logger.Info("successfully uploaded data to S3",
		zap.String("s3_uri", s3URI),
		zap.Int("size_bytes", len(data)))

	return s3URI, nil
}
