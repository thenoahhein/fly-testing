# flyd-lite

A simplified container orchestrator that downloads container images from S3, unpacks them into devicemapper thin volumes, and creates snapshots for activation. This is a model implementation of the core functionality in Fly.io's flyd orchestrator.

## Overview

flyd-lite implements a finite state machine (FSM) that manages container image lifecycle through the following states:

1. **new** → **fetched**: Download image tarball from S3 with checksum validation
2. **fetched** → **unpacked**: Unpack image layers into a devicemapper thin volume
3. **unpacked** → **activated**: Create a read-write snapshot for container execution

## Architecture

### Components

- **FSM Library**: Uses Fly.io's FSMv2 library for state management and orchestration
- **SQLite Database**: Tracks image metadata and provides distributed locking
- **S3 Client**: Downloads container images with checksum validation
- **DeviceMapper**: Manages thin pool devices and snapshots
- **Security Layer**: Validates paths, sanitizes commands, and enforces resource limits

### Key Features

- **Idempotent Operations**: All operations can be safely retried
- **Distributed Locking**: SQLite-based locking prevents race conditions
- **Security Hardening**: Path validation, command restrictions, and resource limits
- **Checksum Verification**: SHA-256 validation of downloaded blobs
- **Graceful Cleanup**: Proper resource cleanup on shutdown or failure

## Prerequisites

- Linux system with root privileges
- DeviceMapper support
- Required system tools: `fallocate`, `losetup`, `dmsetup`, `mkfs.ext4`, `mount`, `umount`
- Go 1.23+ for building

## Setup

### 1. Build the Application

```bash
cd /path/to/flyd-lite
go mod tidy
go build -o flyd-lite .
```

### 2. Setup DeviceMapper Pool (One-time)

```bash
# Create pool files
sudo fallocate -l 1M /var/lib/flyd/pool_meta
sudo fallocate -l 2G /var/lib/flyd/pool_data

# Setup loop devices
METADATA_DEV="$(sudo losetup -f --show /var/lib/flyd/pool_meta)"
DATA_DEV="$(sudo losetup -f --show /var/lib/flyd/pool_data)"

# Create thin pool
sudo dmsetup create --verifyudev pool --table "0 4194304 thin-pool ${METADATA_DEV} ${DATA_DEV} 2048 32768"
```

### 3. Configure AWS Credentials

```bash
# Set AWS credentials for S3 access
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
# Or use IAM roles, credential files, etc.
```

## Usage

```bash
# Process a container image
sudo ./flyd-lite images/redis_latest.tar
```

The application will:

1. Download the specified image from `s3://flyio-platform-hiring-challenge/images/`
2. Validate the checksum
3. Unpack layers into a thin volume
4. Create a snapshot device for activation
5. Return the snapshot device path

### Example Output

```
INFO[2024-01-15T10:30:00Z] Starting image processing object_key=images/redis_latest.tar
INFO[2024-01-15T10:30:01Z] Image processing FSM started object_key=images/redis_latest.tar version=01HXYZ...
SUCCESS: Image activated at device /dev/mapper/snap-01HXYZ...
```

## FSM State Transitions

### State: new → fetched

**Action**: `fetchBlob`

- Downloads image tarball from S3
- Validates SHA-256 checksum
- Stores in temporary directory
- Updates database state

**Idempotency**: Skips download if checksum matches existing file

### State: fetched → unpacked

**Action**: `unpackToPool`

- Creates devicemapper thin device
- Formats with ext4 filesystem
- Mounts device and unpacks container layers
- Handles Docker whiteouts and layer merging
- Unmounts and updates database

**Idempotency**: Skips unpacking if device already exists

### State: unpacked → activated

**Action**: `activateSnapshot`

- Creates read-write snapshot of base device
- Updates database with snapshot device path
- Ready for container runtime mounting

**Idempotency**: Returns existing snapshot if already created

## Database Schema

```sql
CREATE TABLE images (
    id TEXT PRIMARY KEY,           -- ULID
    object_key TEXT UNIQUE,        -- S3 path
    sha256 TEXT,                   -- Checksum
    state TEXT,                    -- FSM state
    pool_device TEXT,              -- Base device path
    snap_device TEXT,              -- Snapshot device path
    created_at DATETIME,
    updated_at DATETIME
);

CREATE TABLE _busy (
    name TEXT PRIMARY KEY          -- Distributed locks
);
```

## Security Features

### Path Validation

- All paths validated against allowed directories
- Directory traversal protection
- No absolute paths outside allowed roots

### Command Security

- Restricted command execution
- Sanitized environment variables
- Process group management for cleanup
- Command timeouts

### Resource Limits

- Maximum file size validation
- Disk space checks
- DeviceMapper pool monitoring
- Temporary file cleanup

### Input Validation

- S3 object key validation
- Checksum verification
- Archive format validation
- Layer extraction safety

## Error Handling

- **Checksum Mismatch**: Download rejected, temporary files cleaned
- **Insufficient Space**: Operation aborted with clear error
- **DeviceMapper Errors**: Pool status validation and cleanup
- **FSM Failures**: Automatic retry with exponential backoff

## Monitoring

The application emits structured JSON logs with:

- FSM state transitions
- Performance metrics
- Error details
- Security events
- Resource usage

## Cleanup

```bash
# Remove thin pool and loop devices
sudo dmsetup remove pool
sudo losetup -d /dev/loop0  # Adjust device names as needed
sudo losetup -d /dev/loop1

# Clean data files
sudo rm -rf /var/lib/flyd
```

## Development

### Running Tests

```bash
go test ./...
```

### Key Files

- `main.go`: Application entry point and orchestration
- `fsm_transitions.go`: FSM state transition implementations
- `database.go`: SQLite persistence and locking
- `s3_client.go`: S3 download with checksum validation
- `devicemapper.go`: Thin pool and snapshot management
- `unpacker.go`: Container image layer extraction
- `security.go`: Security validation and hardening
- `types.go`: Data structures and constants

## License

This is a demonstration implementation for Fly.io's hiring challenge.