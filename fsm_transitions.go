package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/superfly/fsm"
)

// Context keys for service injection
type serviceContextKey string

const (
	s3ClientKey        serviceContextKey = "s3_client"
	deviceMapperKey    serviceContextKey = "device_mapper"
)

// WithS3Client adds S3 client to context
func WithS3Client(ctx context.Context, client *S3Client) context.Context {
	return context.WithValue(ctx, s3ClientKey, client)
}

// GetS3Client retrieves S3 client from context
func GetS3Client(ctx context.Context) *S3Client {
	if client, ok := ctx.Value(s3ClientKey).(*S3Client); ok {
		return client
	}
	return nil
}

// WithDeviceMapper adds DeviceMapper to context
func WithDeviceMapper(ctx context.Context, dm *DeviceMapper) context.Context {
	return context.WithValue(ctx, deviceMapperKey, dm)
}

// GetDeviceMapper retrieves DeviceMapper from context
func GetDeviceMapper(ctx context.Context) *DeviceMapper {
	if dm, ok := ctx.Value(deviceMapperKey).(*DeviceMapper); ok {
		return dm
	}
	return nil
}

// RegisterImageFSM registers the image processing FSM with the manager
func RegisterImageFSM(manager *fsm.Manager) (fsm.Start[ImageRequest, ImageResponse], fsm.Resume, error) {
	return fsm.Register[ImageRequest, ImageResponse](manager, "process").
		Start(StateNew, fetchBlob).
		To(StateFetched, unpackToPool).
		To(StateUnpacked, activateSnapshot).
		End(StateActivated).
		Build(context.Background())
}

// fetchBlob downloads the image blob from S3 if not already present
func fetchBlob(ctx context.Context, req *fsm.Request[ImageRequest, ImageResponse]) (*fsm.Response[ImageResponse], error) {
	logger := req.Log().WithField("transition", "fetch_blob")
	logger.Info("starting blob fetch transition")

	db := GetDatabase(ctx)
	s3Client := GetS3Client(ctx)
	
	if db == nil || s3Client == nil {
		return nil, fmt.Errorf("missing required services in context")
	}

	objectKey := req.Msg.Key
	lockName := fmt.Sprintf("fetch_%s", objectKey)

	// Try to acquire lock
	locked, err := db.TryLock(ctx, lockName)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !locked {
		logger.Info("another process is handling this image, skipping")
		return &fsm.Response[ImageResponse]{
			Msg: &ImageResponse{Status: "skipped"},
		}, nil
	}
	defer db.ReleaseLock(ctx, lockName)

	// Get expected checksum from S3 metadata
	expectedChecksum, err := s3Client.GetImageChecksum(ctx, objectKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get expected checksum: %w", err)
	}

	// Get or create image record
	img, err := db.GetOrCreateImage(ctx, objectKey, expectedChecksum)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create image record: %w", err)
	}

	// Check if already fetched
	if img.State != StateNew {
		logger.WithField("current_state", img.State).Info("image already processed, skipping fetch")
		return &fsm.Response[ImageResponse]{
			Msg: &ImageResponse{Status: "already_fetched"},
		}, nil
	}

	// Download the blob
	localPath := filepath.Join(TmpDir, img.ID+".tar")
	actualChecksum, err := s3Client.DownloadWithChecksum(ctx, objectKey, localPath)
	if err != nil {
		return nil, fmt.Errorf("failed to download blob: %w", err)
	}

	// Validate checksum
	if actualChecksum != expectedChecksum {
		os.Remove(localPath) // Clean up on checksum mismatch
		return nil, fmt.Errorf("checksum mismatch: expected %s, got %s", expectedChecksum, actualChecksum)
	}

	// Update state in database
	if err := db.UpdateImageState(ctx, img.ID, StateFetched); err != nil {
		return nil, fmt.Errorf("failed to update image state: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"local_path": localPath,
		"checksum":   actualChecksum,
	}).Info("blob fetch completed")

	return &fsm.Response[ImageResponse]{
		Msg: &ImageResponse{Status: "fetched"},
	}, nil
}

// unpackToPool unpacks the image into a devicemapper thin device
func unpackToPool(ctx context.Context, req *fsm.Request[ImageRequest, ImageResponse]) (*fsm.Response[ImageResponse], error) {
	logger := req.Log().WithField("transition", "unpack_to_pool")
	logger.Info("starting unpack to pool transition")

	db := GetDatabase(ctx)
	dm := GetDeviceMapper(ctx)
	
	if db == nil || dm == nil {
		return nil, fmt.Errorf("missing required services in context")
	}

	objectKey := req.Msg.Key

	// Get image record
	img, err := db.GetImage(ctx, objectKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get image record: %w", err)
	}

	// Check if already unpacked
	if img.State != StateFetched {
		if img.State == StateUnpacked || img.State == StateActivated {
			logger.WithField("current_state", img.State).Info("image already unpacked")
			return &fsm.Response[ImageResponse]{
				Msg: &ImageResponse{Status: "already_unpacked"},
			}, nil
		}
		return nil, fmt.Errorf("invalid state for unpacking: %s", img.State)
	}

	// Generate device ID
	deviceID := dm.GenerateDeviceID()
	
	// Create thin device
	devicePath, err := dm.CreateThinDevice(ctx, deviceID)
	if err != nil {
		return nil, fmt.Errorf("failed to create thin device: %w", err)
	}

	// Create filesystem
	if err := dm.MakeFilesystem(ctx, devicePath); err != nil {
		return nil, fmt.Errorf("failed to create filesystem: %w", err)
	}

	// Mount device
	mountpoint := filepath.Join(MountDir, img.ID)
	if err := dm.MountDevice(ctx, devicePath, mountpoint); err != nil {
		return nil, fmt.Errorf("failed to mount device: %w", err)
	}

	// Unpack tarball
	tarPath := filepath.Join(TmpDir, img.ID+".tar")
	if err := UnpackTarball(ctx, tarPath, mountpoint); err != nil {
		dm.UnmountDevice(ctx, mountpoint) // Clean up on failure
		return nil, fmt.Errorf("failed to unpack tarball: %w", err)
	}

	// Unmount device
	if err := dm.UnmountDevice(ctx, mountpoint); err != nil {
		logger.WithError(err).Warn("failed to unmount device after unpacking")
	}

	// Clean up temporary tarball
	os.Remove(tarPath)

	// Update database with device information
	if err := db.UpdateImageDevice(ctx, img.ID, devicePath, ""); err != nil {
		return nil, fmt.Errorf("failed to update image device: %w", err)
	}

	// Update state
	if err := db.UpdateImageState(ctx, img.ID, StateUnpacked); err != nil {
		return nil, fmt.Errorf("failed to update image state: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"device_path": devicePath,
		"device_id":   deviceID,
		"mountpoint":  mountpoint,
	}).Info("unpack to pool completed")

	return &fsm.Response[ImageResponse]{
		Msg: &ImageResponse{Status: "unpacked"},
	}, nil
}

// activateSnapshot creates a snapshot of the base device for activation
func activateSnapshot(ctx context.Context, req *fsm.Request[ImageRequest, ImageResponse]) (*fsm.Response[ImageResponse], error) {
	logger := req.Log().WithField("transition", "activate_snapshot")
	logger.Info("starting activate snapshot transition")

	db := GetDatabase(ctx)
	dm := GetDeviceMapper(ctx)
	
	if db == nil || dm == nil {
		return nil, fmt.Errorf("missing required services in context")
	}

	objectKey := req.Msg.Key

	// Get image record
	img, err := db.GetImage(ctx, objectKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get image record: %w", err)
	}

	// Check if already activated
	if img.State == StateActivated && img.SnapDevice != "" {
		logger.WithField("snap_device", img.SnapDevice).Info("image already activated")
		return &fsm.Response[ImageResponse]{
			Msg: &ImageResponse{
				SnapshotDevice: img.SnapDevice,
				Status:         "already_activated",
			},
		}, nil
	}

	if img.State != StateUnpacked {
		return nil, fmt.Errorf("invalid state for activation: %s", img.State)
	}

	if img.PoolDevice == "" {
		return nil, fmt.Errorf("no pool device found for image")
	}

	// Extract device ID from pool device path
	baseDeviceID := filepath.Base(img.PoolDevice)
	if len(baseDeviceID) > 4 && baseDeviceID[:4] == "img-" {
		baseDeviceID = baseDeviceID[4:] // Remove "img-" prefix
	}

	// Generate snapshot ID
	snapID := dm.GenerateDeviceID()

	// Create snapshot
	snapPath, err := dm.CreateSnapshot(ctx, baseDeviceID, snapID)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Update database with snapshot device
	if err := db.UpdateImageDevice(ctx, img.ID, img.PoolDevice, snapPath); err != nil {
		return nil, fmt.Errorf("failed to update image with snapshot device: %w", err)
	}

	// Update state
	if err := db.UpdateImageState(ctx, img.ID, StateActivated); err != nil {
		return nil, fmt.Errorf("failed to update image state: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"base_device_id": baseDeviceID,
		"snap_id":        snapID,
		"snap_path":      snapPath,
	}).Info("activate snapshot completed")

	return &fsm.Response[ImageResponse]{
		Msg: &ImageResponse{
			SnapshotDevice: snapPath,
			Status:         "activated",
		},
	}, nil
}