package main

import (
	"time"
)

// ImageRequest represents a request to process a container image
type ImageRequest struct {
	Key string `json:"key"` // S3 object key
}

// ImageResponse represents the result of processing an image
type ImageResponse struct {
	SnapshotDevice string `json:"snapshot_device"`
	Status         string `json:"status"`
}

// ImageMetadata represents database record for an image
type ImageMetadata struct {
	ID           string    `db:"id"`
	ObjectKey    string    `db:"object_key"`
	SHA256       string    `db:"sha256"`
	State        string    `db:"state"`
	PoolDevice   string    `db:"pool_device"`
	SnapDevice   string    `db:"snap_device"`
	CreatedAt    time.Time `db:"created_at"`
	UpdatedAt    time.Time `db:"updated_at"`
}

// States for the FSM
const (
	StateNew       = "new"
	StateFetched   = "fetched"
	StateUnpacked  = "unpacked"
	StateActivated = "activated"
)

// DeviceMapper configuration
const (
	PoolMetaFile = "/tmp/flyd/pool_meta"
	PoolDataFile = "/tmp/flyd/pool_data"
	TmpDir       = "/tmp/flyd/tmp"
	MountDir     = "/tmp/flyd/mnt"
	DBPath       = "/tmp/flyd/db.sqlite"
)

// S3 configuration
const (
	S3Bucket = "flyio-platform-hiring-challenge"
	S3Region = "us-east-1"
)