package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/superfly/fsm"
)

func main() {
	// Setup logging
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)

	// Handle command line arguments
	if len(os.Args) < 2 {
		logger.Fatal("Usage: flyd-lite <s3-object-key>")
	}

	objectKey := os.Args[1]

	// Validate the object key
	if err := ValidateImagePath(objectKey); err != nil {
		logger.WithError(err).Fatal("Invalid object key")
	}

	// Setup context with cancellation
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Initialize security configuration
	securityConfig := DefaultSecurityConfig()

	// Setup secure directories
	if err := SetupSecureDirectories(ctx, securityConfig); err != nil {
		logger.WithError(err).Fatal("Failed to setup secure directories")
	}

	// Check system resources
	if err := CheckSystemResources(ctx); err != nil {
		logger.WithError(err).Fatal("Insufficient system resources")
	}

	// Initialize database
	db, err := NewDatabase(DBPath)
	if err != nil {
		logger.WithError(err).Fatal("Failed to initialize database")
	}
	defer db.Close()

	// Initialize S3 client
	s3Client, err := NewS3Client(ctx, S3Bucket)
	if err != nil {
		logger.WithError(err).Fatal("Failed to initialize S3 client")
	}

	// Initialize DeviceMapper
	dm, err := NewDeviceMapper()
	if err != nil {
		logger.WithError(err).Fatal("Failed to initialize DeviceMapper")
	}

	// Setup devicemapper pool
	if err := dm.SetupPool(ctx); err != nil {
		logger.WithError(err).Fatal("Failed to setup devicemapper pool")
	}

	// Validate devicemapper pool health
	if err := ValidateDeviceMapperPool(ctx, dm); err != nil {
		logger.WithError(err).Fatal("DeviceMapper pool health check failed")
	}

	// Create FSM manager
	fsmConfig := fsm.Config{
		Logger: logger.WithField("component", "fsm"),
		DBPath: "/tmp/flyd/fsm",
		Queues: map[string]int{
			"default": 5, // Allow 5 concurrent image processes
		},
	}

	manager, err := fsm.New(fsmConfig)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create FSM manager")
	}

	// Register image processing FSM
	startFSM, resumeFSM, err := RegisterImageFSM(manager)
	if err != nil {
		logger.WithError(err).Fatal("Failed to register image FSM")
	}

	// Resume any existing FSMs
	if err := resumeFSM(ctx); err != nil {
		logger.WithError(err).Error("Failed to resume existing FSMs")
	}

	// Inject dependencies into context
	ctx = WithDatabase(ctx, db)
	ctx = WithS3Client(ctx, s3Client)
	ctx = WithDeviceMapper(ctx, dm)
	ctx = WithLogger(ctx, logger)

	// Start cleanup goroutine
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				if err := CleanupTemporaryFiles(ctx, 24*time.Hour); err != nil {
					logger.WithError(err).Error("Failed to cleanup temporary files")
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Create image request
	req := &ImageRequest{
		Key: objectKey,
	}

	// Start image processing
	logger.WithField("object_key", objectKey).Info("Starting image processing")

	version, err := startFSM(ctx, objectKey, fsm.NewRequest(req, &ImageResponse{}))
	if err != nil {
		logger.WithError(err).Fatal("Failed to start image processing FSM")
	}

	logger.WithFields(logrus.Fields{
		"object_key": objectKey,
		"version":    version.String(),
	}).Info("Image processing FSM started")

	// Wait for completion or cancellation
	if err := manager.Wait(ctx, version); err != nil {
		logger.WithError(err).Error("Image processing failed")
		
		// Cleanup on failure
		cleanup(ctx, logger, manager, dm)
		os.Exit(1)
	}

	// Get the final result
	img, err := db.GetImage(ctx, objectKey)
	if err != nil {
		logger.WithError(err).Error("Failed to get final image result")
		os.Exit(1)
	}

	if img.State == StateActivated && img.SnapDevice != "" {
		fmt.Printf("SUCCESS: Image activated at device %s\n", img.SnapDevice)
		logger.WithFields(logrus.Fields{
			"object_key":      objectKey,
			"snapshot_device": img.SnapDevice,
			"pool_device":     img.PoolDevice,
			"state":          img.State,
		}).Info("Image processing completed successfully")
	} else {
		fmt.Printf("FAILED: Image processing incomplete, state: %s\n", img.State)
		logger.WithFields(logrus.Fields{
			"object_key": objectKey,
			"state":      img.State,
		}).Error("Image processing completed with errors")
		os.Exit(1)
	}

	// Graceful shutdown
	cleanup(ctx, logger, manager, dm)
}

// cleanup performs graceful cleanup
func cleanup(ctx context.Context, logger logrus.FieldLogger, manager *fsm.Manager, dm *DeviceMapper) {
	logger.Info("Starting cleanup")

	// Shutdown FSM manager with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if manager != nil {
		manager.Shutdown(30 * time.Second)
	}

	// Clean temporary files
	if err := CleanupTemporaryFiles(shutdownCtx, 0); err != nil {
		logger.WithError(err).Warn("Failed to cleanup temporary files during shutdown")
	}

	logger.Info("Cleanup completed")
}

// InitializeSystem performs one-time system initialization
func InitializeSystem(ctx context.Context, logger logrus.FieldLogger) error {
	logger.Info("Initializing system")

	// Check if running as root (required for devicemapper operations)
	if os.Geteuid() != 0 {
		return fmt.Errorf("flyd-lite must be run as root for devicemapper operations")
	}

	// Verify required commands are available
	requiredCommands := []string{
		"fallocate", "losetup", "dmsetup", "mkfs.ext4", "mount", "umount",
	}

	for _, cmd := range requiredCommands {
		if _, err := os.Stat("/usr/bin/" + cmd); os.IsNotExist(err) {
			if _, err := os.Stat("/bin/" + cmd); os.IsNotExist(err) {
				if _, err := os.Stat("/sbin/" + cmd); os.IsNotExist(err) {
					return fmt.Errorf("required command not found: %s", cmd)
				}
			}
		}
	}

	logger.Info("System initialization completed")
	return nil
}