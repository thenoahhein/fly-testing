package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
	_ "modernc.org/sqlite"
)

type Database struct {
	db *sql.DB
}

// NewDatabase creates and initializes a new SQLite database
func NewDatabase(dbPath string) (*Database, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Enable WAL mode for better concurrency
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	database := &Database{db: db}
	if err := database.createTables(); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return database, nil
}

func (d *Database) createTables() error {
	schema := `
	CREATE TABLE IF NOT EXISTS images (
		id TEXT PRIMARY KEY,
		object_key TEXT UNIQUE NOT NULL,
		sha256 TEXT NOT NULL,
		state TEXT NOT NULL,
		pool_device TEXT,
		snap_device TEXT,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS _busy (
		name TEXT PRIMARY KEY
	);

	CREATE INDEX IF NOT EXISTS idx_images_object_key ON images(object_key);
	CREATE INDEX IF NOT EXISTS idx_images_state ON images(state);
	`

	if _, err := d.db.Exec(schema); err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	return nil
}

// TryLock attempts to acquire a named lock, returns true if successful
func (d *Database) TryLock(ctx context.Context, name string) (bool, error) {
	result, err := d.db.ExecContext(ctx, 
		`INSERT INTO _busy(name) VALUES (?) ON CONFLICT(name) DO NOTHING`, name)
	if err != nil {
		return false, fmt.Errorf("failed to acquire lock: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to check lock result: %w", err)
	}

	return rowsAffected > 0, nil
}

// ReleaseLock releases a named lock
func (d *Database) ReleaseLock(ctx context.Context, name string) error {
	_, err := d.db.ExecContext(ctx, `DELETE FROM _busy WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	return nil
}

// GetOrCreateImage gets or creates an image record
func (d *Database) GetOrCreateImage(ctx context.Context, objectKey, sha256 string) (*ImageMetadata, error) {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Try to get existing image
	var img ImageMetadata
	var poolDevice, snapDevice sql.NullString
	err = tx.QueryRowContext(ctx, 
		`SELECT id, object_key, sha256, state, pool_device, snap_device, created_at, updated_at 
		FROM images WHERE object_key = ?`, objectKey).
		Scan(&img.ID, &img.ObjectKey, &img.SHA256, &img.State, 
			&poolDevice, &snapDevice, &img.CreatedAt, &img.UpdatedAt)
	
	if poolDevice.Valid {
		img.PoolDevice = poolDevice.String
	}
	if snapDevice.Valid {
		img.SnapDevice = snapDevice.String
	}

	if err == sql.ErrNoRows {
		// Create new image record
		img = ImageMetadata{
			ID:        ulid.Make().String(),
			ObjectKey: objectKey,
			SHA256:    sha256,
			State:     StateNew,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO images (id, object_key, sha256, state, created_at, updated_at) 
			VALUES (?, ?, ?, ?, ?, ?)`,
			img.ID, img.ObjectKey, img.SHA256, img.State, img.CreatedAt, img.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to create image record: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("failed to query image: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &img, nil
}

// UpdateImageState updates the state of an image
func (d *Database) UpdateImageState(ctx context.Context, id, state string) error {
	_, err := d.db.ExecContext(ctx,
		`UPDATE images SET state = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
		state, id)
	if err != nil {
		return fmt.Errorf("failed to update image state: %w", err)
	}
	return nil
}

// UpdateImageDevice updates device information for an image
func (d *Database) UpdateImageDevice(ctx context.Context, id, poolDevice, snapDevice string) error {
	_, err := d.db.ExecContext(ctx,
		`UPDATE images SET pool_device = ?, snap_device = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
		poolDevice, snapDevice, id)
	if err != nil {
		return fmt.Errorf("failed to update image device: %w", err)
	}
	return nil
}

// GetImage retrieves an image by object key
func (d *Database) GetImage(ctx context.Context, objectKey string) (*ImageMetadata, error) {
	var img ImageMetadata
	var poolDevice, snapDevice sql.NullString
	err := d.db.QueryRowContext(ctx,
		`SELECT id, object_key, sha256, state, pool_device, snap_device, created_at, updated_at 
		FROM images WHERE object_key = ?`, objectKey).
		Scan(&img.ID, &img.ObjectKey, &img.SHA256, &img.State, 
			&poolDevice, &snapDevice, &img.CreatedAt, &img.UpdatedAt)

	if err != nil {
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	if poolDevice.Valid {
		img.PoolDevice = poolDevice.String
	}
	if snapDevice.Valid {
		img.SnapDevice = snapDevice.String
	}

	return &img, nil
}

// Close closes the database connection
func (d *Database) Close() error {
	return d.db.Close()
}

// Context keys for database injection
type contextKey string

const dbContextKey contextKey = "database"

// WithDatabase adds database to context
func WithDatabase(ctx context.Context, db *Database) context.Context {
	return context.WithValue(ctx, dbContextKey, db)
}

// GetDatabase retrieves database from context
func GetDatabase(ctx context.Context) *Database {
	if db, ok := ctx.Value(dbContextKey).(*Database); ok {
		return db
	}
	return nil
}

// Logger context injection
const loggerContextKey contextKey = "logger"

// WithLogger adds logger to context
func WithLogger(ctx context.Context, logger logrus.FieldLogger) context.Context {
	return context.WithValue(ctx, loggerContextKey, logger)
}

// GetLogger retrieves logger from context
func GetLogger(ctx context.Context) logrus.FieldLogger {
	if logger, ok := ctx.Value(loggerContextKey).(logrus.FieldLogger); ok {
		return logger
	}
	return logrus.New()
}