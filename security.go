package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

// SecurityConfig holds security-related configuration
type SecurityConfig struct {
	MaxFileSize     int64         // Maximum file size to process (bytes)
	MaxTarSize      int64         // Maximum tar file size (bytes)
	CommandTimeout  time.Duration // Timeout for shell commands
	AllowedPaths    []string      // Allowed base paths for operations
	BlockedCommands []string      // Commands that should never be executed
}

// DefaultSecurityConfig returns a secure default configuration
func DefaultSecurityConfig() *SecurityConfig {
	return &SecurityConfig{
		MaxFileSize:    5 * 1024 * 1024 * 1024, // 5GB
		MaxTarSize:     2 * 1024 * 1024 * 1024, // 2GB
		CommandTimeout: 10 * time.Minute,
		AllowedPaths: []string{
			"/var/lib/flyd",
			"/mnt/flyd",
			"/tmp/flyd",
			"/dev/mapper",
		},
		BlockedCommands: []string{
			"rm", "rmdir", "mv", "cp", "chmod", "chown", "su", "sudo",
			"passwd", "useradd", "userdel", "usermod", "groupadd",
			"service", "systemctl", "init", "reboot", "shutdown",
		},
	}
}

// ValidateFileSize checks if a file size is within limits
func (sc *SecurityConfig) ValidateFileSize(size int64, fileType string) error {
	var limit int64
	switch fileType {
	case "tar":
		limit = sc.MaxTarSize
	default:
		limit = sc.MaxFileSize
	}

	if size > limit {
		return fmt.Errorf("file size %d exceeds limit %d for type %s", size, limit, fileType)
	}
	return nil
}

// ValidatePath ensures a path is within allowed directories
func (sc *SecurityConfig) ValidatePath(path string) error {
	cleanPath := filepath.Clean(path)
	
	// Check for directory traversal
	if strings.Contains(cleanPath, "..") {
		return fmt.Errorf("path contains directory traversal: %s", path)
	}

	// Must be within allowed paths
	for _, allowedPath := range sc.AllowedPaths {
		if strings.HasPrefix(cleanPath, allowedPath) {
			return nil
		}
	}

	return fmt.Errorf("path %s is not within allowed directories", path)
}

// ValidateCommand checks if a command is safe to execute
func (sc *SecurityConfig) ValidateCommand(cmd string) error {
	command := filepath.Base(cmd)
	
	for _, blocked := range sc.BlockedCommands {
		if command == blocked {
			return fmt.Errorf("command %s is blocked for security reasons", command)
		}
	}
	
	return nil
}

// SecureCommand creates a secure command with timeout and restricted environment
func (sc *SecurityConfig) SecureCommand(ctx context.Context, name string, args ...string) (*exec.Cmd, error) {
	// Validate command
	if err := sc.ValidateCommand(name); err != nil {
		return nil, err
	}

	// Create context with timeout
	cmdCtx, cancel := context.WithTimeout(ctx, sc.CommandTimeout)
	
	// Create command
	cmd := exec.CommandContext(cmdCtx, name, args...)
	
	// Set secure environment
	cmd.Env = []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"LC_ALL=C",
	}
	
	// Set process group for cleanup
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	// Cleanup function
	go func() {
		<-cmdCtx.Done()
		if cmd.Process != nil {
			// Kill the entire process group
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		cancel()
	}()

	return cmd, nil
}

// SanitizeLogOutput removes sensitive information from log output
func SanitizeLogOutput(output string) string {
	// Remove potential sensitive information
	sanitized := output
	
	// Remove AWS credentials if present
	if strings.Contains(strings.ToLower(sanitized), "aws") {
		lines := strings.Split(sanitized, "\n")
		var cleanLines []string
		for _, line := range lines {
			if !containsSensitiveAWS(line) {
				cleanLines = append(cleanLines, line)
			} else {
				cleanLines = append(cleanLines, "[REDACTED: AWS credential]")
			}
		}
		sanitized = strings.Join(cleanLines, "\n")
	}
	
	// Remove file paths that might contain sensitive info
	if strings.Contains(sanitized, "/home/") || strings.Contains(sanitized, "/root/") {
		sanitized = strings.ReplaceAll(sanitized, "/home/", "/[HOME]/")
		sanitized = strings.ReplaceAll(sanitized, "/root/", "/[ROOT]/")
	}
	
	return sanitized
}

// containsSensitiveAWS checks if a line contains AWS credentials
func containsSensitiveAWS(line string) bool {
	lower := strings.ToLower(line)
	sensitivePatterns := []string{
		"aws_access_key",
		"aws_secret_key",
		"accesskeyid",
		"secretaccesskey",
		"sessiontoken",
	}
	
	for _, pattern := range sensitivePatterns {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}

// ValidateImagePath validates that an image path is safe
func ValidateImagePath(imagePath string) error {
	// Clean the path
	cleanPath := filepath.Clean(imagePath)
	
	// Must not be absolute unless in allowed directories
	if filepath.IsAbs(cleanPath) {
		return fmt.Errorf("absolute image paths not allowed: %s", imagePath)
	}
	
	// Must not contain directory traversal
	if strings.Contains(cleanPath, "..") {
		return fmt.Errorf("image path contains directory traversal: %s", imagePath)
	}
	
	// Must be within images prefix
	if !strings.HasPrefix(cleanPath, "images/") && cleanPath != "images" {
		return fmt.Errorf("image path must be under images/ directory: %s", imagePath)
	}
	
	// Must end with reasonable file extension
	if !strings.HasSuffix(strings.ToLower(cleanPath), ".tar") &&
		!strings.HasSuffix(strings.ToLower(cleanPath), ".tar.gz") &&
		!strings.HasSuffix(strings.ToLower(cleanPath), ".tgz") {
		return fmt.Errorf("image path must have valid archive extension: %s", imagePath)
	}
	
	return nil
}

// CheckSystemResources verifies system has sufficient resources
func CheckSystemResources(ctx context.Context) error {
	logger := GetLogger(ctx).WithField("component", "security")
	
	// Check available disk space
	var stat syscall.Statfs_t
	if err := syscall.Statfs("/tmp/flyd", &stat); err != nil {
		return fmt.Errorf("failed to check disk space: %w", err)
	}
	
	availableSpace := stat.Bavail * uint64(stat.Bsize)
	requiredSpace := uint64(10 * 1024 * 1024 * 1024) // 10GB
	
	if availableSpace < requiredSpace {
		return fmt.Errorf("insufficient disk space: have %d bytes, need %d bytes", 
			availableSpace, requiredSpace)
	}
	
	logger.WithFields(logrus.Fields{
		"available_space_gb": availableSpace / (1024 * 1024 * 1024),
		"required_space_gb":  requiredSpace / (1024 * 1024 * 1024),
	}).Info("disk space check passed")
	
	return nil
}

// ValidateDeviceMapperPool checks devicemapper pool health
func ValidateDeviceMapperPool(ctx context.Context, dm *DeviceMapper) error {
	logger := GetLogger(ctx).WithField("component", "security")
	
	// Get pool status
	status, err := dm.GetPoolStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pool status: %w", err)
	}
	
	// Parse status to check usage (simplified check)
	if strings.Contains(status, "out_of_data_space") {
		return fmt.Errorf("devicemapper pool is out of data space")
	}
	
	if strings.Contains(status, "out_of_metadata_space") {
		return fmt.Errorf("devicemapper pool is out of metadata space")
	}
	
	logger.Info("devicemapper pool health check passed")
	return nil
}

// SetupSecureDirectories creates and secures required directories
func SetupSecureDirectories(ctx context.Context, config *SecurityConfig) error {
	logger := GetLogger(ctx).WithField("component", "security")
	
	dirs := []string{
		"/tmp/flyd",
		"/tmp/flyd/tmp",
		"/tmp/flyd/mnt",
	}
	
	for _, dir := range dirs {
		// Validate path
		if err := config.ValidatePath(dir); err != nil {
			return fmt.Errorf("invalid directory path %s: %w", dir, err)
		}
		
		// Create directory with secure permissions
		if err := os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
		
		// Set ownership and permissions
		if err := os.Chmod(dir, 0750); err != nil {
			logger.WithError(err).WithField("dir", dir).Warn("failed to set directory permissions")
		}
		
		logger.WithField("dir", dir).Info("directory created and secured")
	}
	
	return nil
}

// CleanupTemporaryFiles removes temporary files older than specified duration
func CleanupTemporaryFiles(ctx context.Context, maxAge time.Duration) error {
	logger := GetLogger(ctx).WithField("component", "security")
	
	tmpDir := "/tmp/flyd/tmp"
	
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Directory doesn't exist, nothing to clean
		}
		return fmt.Errorf("failed to read temporary directory: %w", err)
	}
	
	cutoff := time.Now().Add(-maxAge)
	cleaned := 0
	
	for _, entry := range entries {
		filePath := filepath.Join(tmpDir, entry.Name())
		
		info, err := entry.Info()
		if err != nil {
			logger.WithError(err).WithField("file", filePath).Warn("failed to get file info")
			continue
		}
		
		if info.ModTime().Before(cutoff) {
			if err := os.Remove(filePath); err != nil {
				logger.WithError(err).WithField("file", filePath).Warn("failed to remove old temporary file")
			} else {
				cleaned++
				logger.WithField("file", filePath).Debug("removed old temporary file")
			}
		}
	}
	
	if cleaned > 0 {
		logger.WithField("files_cleaned", cleaned).Info("cleaned up temporary files")
	}
	
	return nil
}