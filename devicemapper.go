package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
)

type DeviceMapper struct {
	poolName   string
	metaDevice string
	dataDevice string
}

// NewDeviceMapper creates a new DeviceMapper instance
func NewDeviceMapper() (*DeviceMapper, error) {
	return &DeviceMapper{
		poolName: "pool",
	}, nil
}

// SetupPool initializes the devicemapper thin pool
func (dm *DeviceMapper) SetupPool(ctx context.Context) error {
	logger := GetLogger(ctx).WithField("component", "devicemapper")
	
	logger.Info("setting up devicemapper thin pool")

	// Create pool files if they don't exist
	if err := dm.createPoolFiles(ctx); err != nil {
		return fmt.Errorf("failed to create pool files: %w", err)
	}

	// Set up loop devices
	if err := dm.setupLoopDevices(ctx); err != nil {
		return fmt.Errorf("failed to setup loop devices: %w", err)
	}

	// Create thin pool if it doesn't exist
	if err := dm.createThinPool(ctx); err != nil {
		return fmt.Errorf("failed to create thin pool: %w", err)
	}

	logger.Info("devicemapper thin pool setup completed")
	return nil
}

func (dm *DeviceMapper) createPoolFiles(ctx context.Context) error {
	logger := GetLogger(ctx)

	// Ensure directory exists - use TmpDir base path
	baseDir := "/tmp/flyd"
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Create metadata file (1MB)
	if _, err := os.Stat(PoolMetaFile); os.IsNotExist(err) {
		logger.Info("creating pool metadata file")
		cmd := exec.CommandContext(ctx, "fallocate", "-l", "1M", PoolMetaFile)
		if err := dm.runCommand(ctx, cmd, "create pool metadata file"); err != nil {
			return err
		}
	}

	// Create data file (2GB)
	if _, err := os.Stat(PoolDataFile); os.IsNotExist(err) {
		logger.Info("creating pool data file")
		cmd := exec.CommandContext(ctx, "fallocate", "-l", "2G", PoolDataFile)
		if err := dm.runCommand(ctx, cmd, "create pool data file"); err != nil {
			return err
		}
	}

	return nil
}

func (dm *DeviceMapper) setupLoopDevices(ctx context.Context) error {
	logger := GetLogger(ctx)

	// Check if loop devices are already set up
	if dm.metaDevice != "" && dm.dataDevice != "" {
		return nil
	}

	// Clean up any existing loop devices for these files
	if err := dm.cleanupExistingLoopDevices(ctx); err != nil {
		logger.WithError(err).Warn("failed to cleanup existing loop devices")
	}

	// Set up metadata loop device
	if dm.metaDevice == "" {
		logger.Info("setting up metadata loop device")
		cmd := exec.CommandContext(ctx, "losetup", "-f", "--show", PoolMetaFile)
		output, err := cmd.Output()
		if err != nil {
			// Log the error details for debugging
			if exitErr, ok := err.(*exec.ExitError); ok {
				logger.WithFields(logrus.Fields{
					"error": err,
					"stderr": string(exitErr.Stderr),
				}).Error("losetup command failed")
			}
			return fmt.Errorf("failed to setup metadata loop device: %w", err)
		}
		dm.metaDevice = strings.TrimSpace(string(output))
		logger.WithField("device", dm.metaDevice).Info("metadata loop device created")
	}

	// Set up data loop device
	if dm.dataDevice == "" {
		logger.Info("setting up data loop device")
		cmd := exec.CommandContext(ctx, "losetup", "-f", "--show", PoolDataFile)
		output, err := cmd.Output()
		if err != nil {
			// Log the error details for debugging
			if exitErr, ok := err.(*exec.ExitError); ok {
				logger.WithFields(logrus.Fields{
					"error": err,
					"stderr": string(exitErr.Stderr),
				}).Error("losetup command failed")
			}
			return fmt.Errorf("failed to setup data loop device: %w", err)
		}
		dm.dataDevice = strings.TrimSpace(string(output))
		logger.WithField("device", dm.dataDevice).Info("data loop device created")
	}

	return nil
}

// cleanupExistingLoopDevices detaches any loop devices associated with our pool files
func (dm *DeviceMapper) cleanupExistingLoopDevices(ctx context.Context) error {
	logger := GetLogger(ctx)

	// Get list of all loop devices and their backing files
	cmd := exec.CommandContext(ctx, "losetup", "-l")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to list loop devices: %w", err)
	}

	// Parse output to find devices using our files
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.Contains(line, PoolMetaFile) || strings.Contains(line, PoolDataFile) {
			// Extract device name (first column)
			fields := strings.Fields(line)
			if len(fields) > 0 {
				device := fields[0]
				logger.WithField("device", device).Info("detaching existing loop device")
				
				// Detach the device
				detachCmd := exec.CommandContext(ctx, "losetup", "-d", device)
				if err := detachCmd.Run(); err != nil {
					logger.WithError(err).WithField("device", device).Warn("failed to detach loop device")
				}
			}
		}
	}

	return nil
}

func (dm *DeviceMapper) createThinPool(ctx context.Context) error {
	logger := GetLogger(ctx)

	// Check if thin-pool target is available
	cmd := exec.CommandContext(ctx, "dmsetup", "targets")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to check dmsetup targets: %w", err)
	}
	
	if !strings.Contains(string(output), "thin-pool") {
		logger.Warn("thin-pool target not available, using simple linear mapping as fallback")
		// Create a simple linear device mapping instead
		return dm.createLinearPool(ctx)
	}

	// Check if pool already exists
	cmd = exec.CommandContext(ctx, "dmsetup", "info", dm.poolName)
	if err := cmd.Run(); err == nil {
		logger.Info("thin pool already exists")
		return nil
	}

	logger.Info("creating thin pool")
	table := fmt.Sprintf("0 4194304 thin-pool %s %s 2048 32768", dm.metaDevice, dm.dataDevice)
	cmd = exec.CommandContext(ctx, "dmsetup", "create", "--verifyudev", dm.poolName, "--table", table)
	
	return dm.runCommand(ctx, cmd, "create thin pool")
}

// createLinearPool creates a simple linear device as fallback when thin-pool is not available
func (dm *DeviceMapper) createLinearPool(ctx context.Context) error {
	logger := GetLogger(ctx)

	// Check if pool already exists
	cmd := exec.CommandContext(ctx, "dmsetup", "info", dm.poolName)
	if err := cmd.Run(); err == nil {
		logger.Info("linear pool already exists")
		return nil
	}

	logger.Info("creating linear pool as fallback")
	// Use the data device directly with linear mapping
	table := fmt.Sprintf("0 4194304 linear %s 0", dm.dataDevice)
	cmd = exec.CommandContext(ctx, "dmsetup", "create", "--verifyudev", dm.poolName, "--table", table)
	
	return dm.runCommand(ctx, cmd, "create linear pool")
}

// CreateThinDevice creates a new thin device (or linear device as fallback)
func (dm *DeviceMapper) CreateThinDevice(ctx context.Context, deviceID string) (string, error) {
	logger := GetLogger(ctx).WithField("device_id", deviceID)
	
	devicePath := fmt.Sprintf("/dev/mapper/img-%s", deviceID)
	
	// Check if device already exists
	if _, err := os.Stat(devicePath); err == nil {
		logger.Info("device already exists")
		return devicePath, nil
	}

	logger.Info("creating device")
	
	// Check if we're using thin-pool or linear fallback
	cmd := exec.CommandContext(ctx, "dmsetup", "targets")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to check dmsetup targets: %w", err)
	}
	
	deviceName := fmt.Sprintf("img-%s", deviceID)
	size := "2097152" // 1GB in 512-byte sectors
	
	if strings.Contains(string(output), "thin-pool") {
		// Use thin provisioning
		cmd = exec.CommandContext(ctx, "dmsetup", "message", dm.poolName, "0", fmt.Sprintf("create_thin %s", deviceID))
		if err := dm.runCommand(ctx, cmd, "create thin device"); err != nil {
			return "", err
		}
		
		table := fmt.Sprintf("0 %s thin /dev/mapper/%s %s", size, dm.poolName, deviceID)
		cmd = exec.CommandContext(ctx, "dmsetup", "create", deviceName, "--table", table)
	} else {
		// Use linear mapping as fallback - map directly to the data loop device
		table := fmt.Sprintf("0 %s linear %s 0", size, dm.dataDevice)
		cmd = exec.CommandContext(ctx, "dmsetup", "create", deviceName, "--table", table)
	}
	
	if err := dm.runCommand(ctx, cmd, "create device mapper entry"); err != nil {
		return "", err
	}

	// Wait for device node to appear in /dev/mapper/
	if err := dm.waitForDeviceNode(ctx, devicePath); err != nil {
		return "", fmt.Errorf("device node did not appear: %w", err)
	}

	logger.WithField("device_path", devicePath).Info("device created")
	return devicePath, nil
}

// CreateSnapshot creates a snapshot of a thin device (or copy for linear fallback)
func (dm *DeviceMapper) CreateSnapshot(ctx context.Context, baseDeviceID, snapID string) (string, error) {
	logger := GetLogger(ctx).WithFields(logrus.Fields{
		"base_device_id": baseDeviceID,
		"snap_id":        snapID,
	})

	snapPath := fmt.Sprintf("/dev/mapper/snap-%s", snapID)

	// Check if snapshot already exists
	if _, err := os.Stat(snapPath); err == nil {
		logger.Info("snapshot already exists")
		return snapPath, nil
	}

	logger.Info("creating snapshot")

	// Check if we're using thin-pool or linear fallback
	cmd := exec.CommandContext(ctx, "dmsetup", "targets")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to check dmsetup targets: %w", err)
	}
	
	deviceName := fmt.Sprintf("snap-%s", snapID)
	size := "2097152" // 1GB in 512-byte sectors

	if strings.Contains(string(output), "thin-pool") {
		// Use thin provisioning snapshots
		cmd = exec.CommandContext(ctx, "dmsetup", "message", dm.poolName, "0", 
			fmt.Sprintf("create_snap %s %s", snapID, baseDeviceID))
		if err := dm.runCommand(ctx, cmd, "create snapshot"); err != nil {
			return "", err
		}
		
		table := fmt.Sprintf("0 %s thin /dev/mapper/%s %s", size, dm.poolName, snapID)
		cmd = exec.CommandContext(ctx, "dmsetup", "create", deviceName, "--table", table)
	} else {
		// For linear fallback, just create another linear device pointing to the same backing store
		// This isn't a true snapshot but will work for basic functionality
		logger.Warn("creating linear device as snapshot fallback (not a true snapshot)")
		table := fmt.Sprintf("0 %s linear %s 0", size, dm.dataDevice)
		cmd = exec.CommandContext(ctx, "dmsetup", "create", deviceName, "--table", table)
	}
	
	if err := dm.runCommand(ctx, cmd, "create snapshot device mapper entry"); err != nil {
		return "", err
	}

	// Wait for device node to appear in /dev/mapper/
	if err := dm.waitForDeviceNode(ctx, snapPath); err != nil {
		return "", fmt.Errorf("snapshot device node did not appear: %w", err)
	}

	logger.WithField("snap_path", snapPath).Info("snapshot created")
	return snapPath, nil
}

// MakeFilesystem creates an ext4 filesystem on a device
func (dm *DeviceMapper) MakeFilesystem(ctx context.Context, devicePath string) error {
	logger := GetLogger(ctx).WithField("device", devicePath)
	
	logger.Info("creating ext4 filesystem")
	cmd := exec.CommandContext(ctx, "mkfs.ext4", "-F", devicePath)
	return dm.runCommand(ctx, cmd, "create filesystem")
}

// waitForDeviceNode waits for a device node to appear in /dev/mapper/
func (dm *DeviceMapper) waitForDeviceNode(ctx context.Context, devicePath string) error {
	logger := GetLogger(ctx).WithField("device_path", devicePath)
	
	// Wait up to 10 seconds for device to appear
	for i := 0; i < 100; i++ {
		if _, err := os.Stat(devicePath); err == nil {
			return nil
		}
		
		// Wait 100ms before checking again
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Continue waiting
		}
	}
	
	logger.Error("device node did not appear within timeout")
	return fmt.Errorf("device node %s did not appear within 10 seconds", devicePath)
}

// MountDevice mounts a device to a mountpoint
func (dm *DeviceMapper) MountDevice(ctx context.Context, devicePath, mountpoint string) error {
	logger := GetLogger(ctx).WithFields(logrus.Fields{
		"device":     devicePath,
		"mountpoint": mountpoint,
	})

	// Create mountpoint directory
	if err := os.MkdirAll(mountpoint, 0755); err != nil {
		return fmt.Errorf("failed to create mountpoint: %w", err)
	}

	// Check if already mounted
	cmd := exec.CommandContext(ctx, "mountpoint", "-q", mountpoint)
	if err := cmd.Run(); err == nil {
		logger.Info("device already mounted")
		return nil
	}

	logger.Info("mounting device")
	cmd = exec.CommandContext(ctx, "mount", devicePath, mountpoint)
	return dm.runCommand(ctx, cmd, "mount device")
}

// UnmountDevice unmounts a device
func (dm *DeviceMapper) UnmountDevice(ctx context.Context, mountpoint string) error {
	logger := GetLogger(ctx).WithField("mountpoint", mountpoint)
	
	// Check if mounted
	cmd := exec.CommandContext(ctx, "mountpoint", "-q", mountpoint)
	if err := cmd.Run(); err != nil {
		logger.Info("device not mounted")
		return nil
	}

	logger.Info("unmounting device")
	cmd = exec.CommandContext(ctx, "umount", mountpoint)
	return dm.runCommand(ctx, cmd, "unmount device")
}

// GenerateDeviceID generates a unique device ID using ULID
func (dm *DeviceMapper) GenerateDeviceID() string {
	return ulid.Make().String()
}

// GetPoolStatus returns the status of the thin pool
func (dm *DeviceMapper) GetPoolStatus(ctx context.Context) (string, error) {
	cmd := exec.CommandContext(ctx, "dmsetup", "status", dm.poolName)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get pool status: %w", err)
	}
	return string(output), nil
}

// runCommand runs a command with proper logging and error handling
func (dm *DeviceMapper) runCommand(ctx context.Context, cmd *exec.Cmd, operation string) error {
	logger := GetLogger(ctx).WithFields(logrus.Fields{
		"operation": operation,
		"command":   strings.Join(cmd.Args, " "),
	})

	logger.Debug("running command")
	
	// Set environment variables for security
	cmd.Env = []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.WithFields(logrus.Fields{
			"error":  err,
			"output": string(output),
		}).Error("command failed")
		return fmt.Errorf("%s failed: %w", operation, err)
	}

	if len(output) > 0 {
		logger.WithField("output", string(output)).Debug("command output")
	}

	return nil
}

// Cleanup cleans up devicemapper resources
func (dm *DeviceMapper) Cleanup(ctx context.Context) error {
	logger := GetLogger(ctx).WithField("component", "devicemapper")
	
	logger.Info("cleaning up devicemapper resources")
	
	// Remove thin pool
	if err := exec.CommandContext(ctx, "dmsetup", "remove", dm.poolName).Run(); err != nil {
		logger.WithError(err).Warn("failed to remove thin pool")
	}

	// Detach loop devices
	if dm.metaDevice != "" {
		if err := exec.CommandContext(ctx, "losetup", "-d", dm.metaDevice).Run(); err != nil {
			logger.WithError(err).WithField("device", dm.metaDevice).Warn("failed to detach metadata loop device")
		}
	}

	if dm.dataDevice != "" {
		if err := exec.CommandContext(ctx, "losetup", "-d", dm.dataDevice).Run(); err != nil {
			logger.WithError(err).WithField("device", dm.dataDevice).Warn("failed to detach data loop device")
		}
	}

	return nil
}