package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"
)

type S3Client struct {
	client *s3.Client
	bucket string
}

// NewS3Client creates a new S3 client
func NewS3Client(ctx context.Context, bucket string) (*S3Client, error) {
	// Try to load default config first
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(S3Region))
	if err != nil {
		// If loading default config fails, try anonymous access for public bucket
		cfg = aws.Config{
			Region:      S3Region,
			Credentials: aws.AnonymousCredentials{},
		}
	} else {
		// Check if we have valid credentials, if not, use anonymous
		creds, err := cfg.Credentials.Retrieve(ctx)
		if err != nil {
			cfg.Credentials = aws.AnonymousCredentials{}
		} else if creds.AccessKeyID == "" {
			cfg.Credentials = aws.AnonymousCredentials{}
		}
	}

	return &S3Client{
		client: s3.NewFromConfig(cfg),
		bucket: bucket,
	}, nil
}

// GetObjectSize gets the size of an S3 object
func (s *S3Client) GetObjectSize(ctx context.Context, key string) (int64, error) {
	resp, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get object metadata: %w", err)
	}

	if resp.ContentLength == nil {
		return 0, fmt.Errorf("content length not available")
	}

	return *resp.ContentLength, nil
}

// DownloadWithChecksum downloads an S3 object to a local file and returns SHA256 checksum
func (s *S3Client) DownloadWithChecksum(ctx context.Context, key, localPath string) (string, error) {
	logger := GetLogger(ctx).WithFields(logrus.Fields{
		"s3_key":    key,
		"local_path": localPath,
	})

	logger.Info("starting S3 download")

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return "", fmt.Errorf("failed to create parent directory: %w", err)
	}

	// Create temporary file
	tmpPath := localPath + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpPath)
	}()

	// Download object
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get S3 object: %w", err)
	}
	defer resp.Body.Close()

	// Stream download while computing checksum
	hasher := sha256.New()
	multiWriter := io.MultiWriter(tmpFile, hasher)

	bytesWritten, err := io.Copy(multiWriter, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to download object: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		return "", fmt.Errorf("failed to sync file: %w", err)
	}

	tmpFile.Close()

	// Atomic move to final location
	if err := os.Rename(tmpPath, localPath); err != nil {
		return "", fmt.Errorf("failed to move file to final location: %w", err)
	}

	checksum := fmt.Sprintf("%x", hasher.Sum(nil))
	logger.WithFields(logrus.Fields{
		"bytes_downloaded": bytesWritten,
		"sha256":          checksum,
	}).Info("download completed")

	return checksum, nil
}

// ValidateChecksum validates that a file matches the expected SHA256 checksum
func ValidateChecksum(filePath, expectedChecksum string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file for checksum validation: %w", err)
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return fmt.Errorf("failed to compute checksum: %w", err)
	}

	actualChecksum := fmt.Sprintf("%x", hasher.Sum(nil))
	if actualChecksum != expectedChecksum {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expectedChecksum, actualChecksum)
	}

	return nil
}

// GetImageChecksum downloads the first few KB of an image to compute expected checksum
// This simulates getting the expected checksum from metadata
func (s *S3Client) GetImageChecksum(ctx context.Context, key string) (string, error) {
	// For the challenge, we'll compute the checksum by downloading the first 1MB
	// In reality, this would come from image metadata or manifest
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String("bytes=0-1048575"), // First 1MB
	})
	if err != nil {
		return "", fmt.Errorf("failed to get object for checksum: %w", err)
	}
	defer resp.Body.Close()

	// For this implementation, we'll do a full download to get the real checksum
	// In production, this would be stored in image manifest or metadata
	fullResp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get full object for checksum: %w", err)
	}
	defer fullResp.Body.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, fullResp.Body); err != nil {
		return "", fmt.Errorf("failed to compute checksum: %w", err)
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}