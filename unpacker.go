package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
)

// ImageManifest represents the structure of manifest.json in container images
type ImageManifest struct {
	Config   string   `json:"Config"`
	RepoTags []string `json:"RepoTags"`
	Layers   []string `json:"Layers"`
}

// UnpackTarball unpacks a container image tarball to a destination directory
func UnpackTarball(ctx context.Context, tarPath, destDir string) error {
	logger := GetLogger(ctx).WithFields(logrus.Fields{
		"tar_path": tarPath,
		"dest_dir": destDir,
	})

	logger.Info("starting tarball unpacking")

	// Open the tarball
	file, err := os.Open(tarPath)
	if err != nil {
		return fmt.Errorf("failed to open tarball: %w", err)
	}
	defer file.Close()

	// Create tar reader
	tarReader := tar.NewReader(file)

	// Extract to a temporary directory first
	tmpDir := destDir + ".tmp"
	if err := os.RemoveAll(tmpDir); err != nil {
		return fmt.Errorf("failed to clean temporary directory: %w", err)
	}
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return fmt.Errorf("failed to create temporary directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// Track manifest and layers
	var manifest []ImageManifest
	layerPaths := make(map[string]string)

	// First pass: extract all files and identify structure
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read tar header: %w", err)
		}

		// Validate path for security
		if err := validatePath(header.Name); err != nil {
			logger.WithField("path", header.Name).Warn("skipping unsafe path")
			continue
		}

		targetPath := filepath.Join(tmpDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", targetPath, err)
			}

		case tar.TypeReg:
			// Ensure parent directory exists
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return fmt.Errorf("failed to create parent directory: %w", err)
			}

			// Extract file
			outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", targetPath, err)
			}

			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return fmt.Errorf("failed to extract file %s: %w", targetPath, err)
			}
			outFile.Close()

			// Track special files
			if header.Name == "manifest.json" {
				if err := loadManifest(targetPath, &manifest); err != nil {
					return fmt.Errorf("failed to load manifest: %w", err)
				}
			} else if strings.HasSuffix(header.Name, ".tar.gz") || strings.HasSuffix(header.Name, "/layer.tar") {
				layerPaths[header.Name] = targetPath
			}

		default:
			logger.WithFields(logrus.Fields{
				"name": header.Name,
				"type": header.Typeflag,
			}).Debug("skipping unsupported file type")
		}
	}

	// Process the container image
	if len(manifest) == 0 {
		return fmt.Errorf("no manifest.json found in tarball")
	}

	// Use the first manifest entry
	imageManifest := manifest[0]
	logger.WithFields(logrus.Fields{
		"config":    imageManifest.Config,
		"repo_tags": imageManifest.RepoTags,
		"layers":    len(imageManifest.Layers),
	}).Info("processing container image")

	// Create the final rootfs
	rootfsDir := filepath.Join(destDir, "rootfs")
	if err := os.MkdirAll(rootfsDir, 0755); err != nil {
		return fmt.Errorf("failed to create rootfs directory: %w", err)
	}

	// Extract layers in order
	for i, layer := range imageManifest.Layers {
		layerPath, exists := layerPaths[layer]
		if !exists {
			return fmt.Errorf("layer %s not found in tarball", layer)
		}

		logger.WithFields(logrus.Fields{
			"layer": layer,
			"index": i,
		}).Info("extracting layer")

		if err := extractLayer(ctx, layerPath, rootfsDir); err != nil {
			return fmt.Errorf("failed to extract layer %s: %w", layer, err)
		}
	}

	// Copy config and manifest to final destination
	configSrc := filepath.Join(tmpDir, imageManifest.Config)
	configDst := filepath.Join(destDir, "config.json")
	if err := copyFile(configSrc, configDst); err != nil {
		logger.WithError(err).Warn("failed to copy config file")
	}

	manifestSrc := filepath.Join(tmpDir, "manifest.json")
	manifestDst := filepath.Join(destDir, "manifest.json")
	if err := copyFile(manifestSrc, manifestDst); err != nil {
		logger.WithError(err).Warn("failed to copy manifest file")
	}

	logger.WithField("rootfs_dir", rootfsDir).Info("tarball unpacking completed")
	return nil
}

// loadManifest loads the manifest.json file
func loadManifest(manifestPath string, manifest *[]ImageManifest) error {
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("failed to read manifest file: %w", err)
	}

	if err := json.Unmarshal(data, manifest); err != nil {
		return fmt.Errorf("failed to parse manifest JSON: %w", err)
	}

	return nil
}

// extractLayer extracts a single layer (tar.gz file) to the destination
func extractLayer(ctx context.Context, layerPath, destDir string) error {
	logger := GetLogger(ctx).WithFields(logrus.Fields{
		"layer_path": layerPath,
		"dest_dir":   destDir,
	})

	// Open layer file
	file, err := os.Open(layerPath)
	if err != nil {
		return fmt.Errorf("failed to open layer: %w", err)
	}
	defer file.Close()

	// Handle both .tar.gz and .tar files
	var reader io.Reader = file
	if strings.HasSuffix(layerPath, ".gz") {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Create tar reader
	tarReader := tar.NewReader(reader)

	// Extract all files in the layer
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read layer tar header: %w", err)
		}

		// Validate and clean path
		if err := validatePath(header.Name); err != nil {
			logger.WithField("path", header.Name).Warn("skipping unsafe path in layer")
			continue
		}

		// Remove leading slash to prevent absolute path issues
		cleanName := strings.TrimPrefix(header.Name, "/")
		if cleanName == "" {
			continue
		}

		targetPath := filepath.Join(destDir, cleanName)

		// Handle whiteouts (Docker layer deletion markers)
		if strings.Contains(filepath.Base(header.Name), ".wh.") {
			if err := handleWhiteout(targetPath, destDir); err != nil {
				logger.WithError(err).WithField("whiteout", header.Name).Warn("failed to handle whiteout")
			}
			continue
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", targetPath, err)
			}

		case tar.TypeReg:
			// Ensure parent directory exists
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return fmt.Errorf("failed to create parent directory: %w", err)
			}

			// Create or overwrite file
			outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", targetPath, err)
			}

			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return fmt.Errorf("failed to extract file %s: %w", targetPath, err)
			}
			outFile.Close()

		case tar.TypeSymlink:
			// Remove existing file/symlink if it exists
			os.Remove(targetPath)
			if err := os.Symlink(header.Linkname, targetPath); err != nil {
				logger.WithError(err).WithField("target", targetPath).Warn("failed to create symlink")
			}

		case tar.TypeLink:
			// Handle hard links
			linkTarget := filepath.Join(destDir, strings.TrimPrefix(header.Linkname, "/"))
			if err := os.Link(linkTarget, targetPath); err != nil {
				logger.WithError(err).WithFields(logrus.Fields{
					"target": targetPath,
					"source": linkTarget,
				}).Warn("failed to create hard link")
			}

		default:
			logger.WithFields(logrus.Fields{
				"name": header.Name,
				"type": header.Typeflag,
			}).Debug("skipping unsupported file type in layer")
		}
	}

	return nil
}

// handleWhiteout processes Docker whiteout files (deletion markers)
func handleWhiteout(whiteoutPath, rootDir string) error {
	// Extract the actual path being deleted
	dir := filepath.Dir(whiteoutPath)
	base := filepath.Base(whiteoutPath)
	
	if !strings.Contains(base, ".wh.") {
		return nil
	}

	// Remove .wh. prefix to get the actual file/directory name
	actualName := strings.Replace(base, ".wh.", "", 1)
	actualPath := filepath.Join(dir, actualName)

	// Remove the file or directory if it exists
	if err := os.RemoveAll(actualPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove whiteout target %s: %w", actualPath, err)
	}

	return nil
}

// validatePath validates that a path is safe (no directory traversal)
func validatePath(path string) error {
	// Clean the path
	cleaned := filepath.Clean(path)
	
	// Check for directory traversal attempts
	if strings.Contains(cleaned, "..") {
		return fmt.Errorf("path contains directory traversal: %s", path)
	}

	// Check for absolute paths that could escape
	if filepath.IsAbs(cleaned) && !strings.HasPrefix(cleaned, "/") {
		return fmt.Errorf("absolute path not allowed: %s", path)
	}

	return nil
}

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	// Create destination directory
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}