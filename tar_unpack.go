package main

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	// security limits for hostile environment protection
	maxTarballSize   = 10 * 1024 * 1024 * 1024 // 10gb compressed tarball max
	maxExtractedSize = 50 * 1024 * 1024 * 1024 // 50gb extracted max (zip bomb protection)
	maxFileCount     = 1000000                 // 1m files max (inode exhaustion protection)
	maxPathDepth     = 100                     // max directory nesting depth
)

// isbadpath checks if a tar entry path is dangerous (absolute or contains ..).
// this prevents path traversal attacks in malicious tarballs.
func isBadPath(p string) bool {
	if filepath.IsAbs(p) {
		return true
	}
	clean := filepath.Clean(p)
	if strings.HasPrefix(clean, "..") {
		return true
	}
	// check nesting depth to prevent resource exhaustion
	depth := strings.Count(clean, string(filepath.Separator))
	if depth > maxPathDepth {
		return true
	}
	return false
}

// iswhiteout checks if a path is an oci whiteout file.
// oci layers use .wh. prefix to indicate deletions from lower layers.
func isWhiteout(name string) (bool, string) {
	base := filepath.Base(name)
	dir := filepath.Dir(name)

	// .wh..wh..opq = opaque whiteout (delete entire directory)
	if base == ".wh..wh..opq" {
		return true, ""
	}

	// .wh.filename = delete specific file/directory
	if strings.HasPrefix(base, ".wh.") {
		targetName := strings.TrimPrefix(base, ".wh.")
		return true, filepath.Join(dir, targetName)
	}

	return false, ""
}

// validatetarball performs a first-pass validation without extracting.
// detects malformed tarballs and collects statistics for security checks.
func validateTarball(tarPath string) error {
	f, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// check compressed size limit
	info, err := os.Stat(tarPath)
	if err != nil {
		return err
	}
	if info.Size() > maxTarballSize {
		return fmt.Errorf("tarball too large: %d bytes (max %d)", info.Size(), maxTarballSize)
	}

	// create reader
	var tr *tar.Reader
	if strings.HasSuffix(tarPath, ".gz") || strings.HasSuffix(tarPath, ".tgz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("invalid gzip: %w", err)
		}
		defer gz.Close()
		tr = tar.NewReader(gz)
	} else {
		tr = tar.NewReader(f)
	}

	// scan headers without extracting
	fileCount := 0
	var estimatedSize int64

	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("corrupt tar header: %w", err)
		}

		fileCount++
		if fileCount > maxFileCount {
			return fmt.Errorf("too many files: %d (max %d)", fileCount, maxFileCount)
		}

		estimatedSize += h.Size
		if estimatedSize > maxExtractedSize {
			return fmt.Errorf("estimated extracted size too large: %d bytes (max %d)",
				estimatedSize, maxExtractedSize)
		}

		// validate path
		if isBadPath(h.Name) {
			return fmt.Errorf("dangerous path in tarball: %s", h.Name)
		}

		// check for hardlink attacks
		if h.Typeflag == tar.TypeLink {
			if isBadPath(h.Linkname) {
				return fmt.Errorf("dangerous hardlink target: %s -> %s", h.Name, h.Linkname)
			}
		}
	}

	return nil
}

// unpacktartodir extracts a tarball (optionally gzipped) into the destination directory.
// supports both plain .tar and .tar.gz/.tgz files.
// validates paths for security and preserves permissions from tar headers.
// implements oci whiteout file handling for proper layer semantics.
func UnpackTarToDir(tarPath, dst string) error {
	// step 1: validate tarball format and security constraints
	if err := validateTarball(tarPath); err != nil {
		return fmt.Errorf("tarball validation failed: %w", err)
	}

	// step 2: open tarball for extraction
	f, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// create tar reader, wrapping with gzip reader if needed
	var tr *tar.Reader
	if strings.HasSuffix(tarPath, ".gz") || strings.HasSuffix(tarPath, ".tgz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer gz.Close()
		tr = tar.NewReader(gz)
	} else {
		tr = tar.NewReader(f)
	}

	// step 3: extract with runtime size tracking
	var totalExtracted int64
	var fileCount int

	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		// increment file counter
		fileCount++
		if fileCount > maxFileCount {
			return fmt.Errorf("file count exceeded during extraction: %d", fileCount)
		}

		// skip dangerous paths (defense in depth, already validated)
		name := h.Name
		if isBadPath(name) {
			continue
		}

		// check for oci whiteout files
		isWh, whTarget := isWhiteout(name)
		if isWh {
			if whTarget == "" {
				// opaque whiteout: delete entire directory
				dirPath := filepath.Join(dst, filepath.Dir(name))
				if err := removeAllSafe(dirPath); err != nil {
					// log but don't fail - directory may not exist
					// (this is a delete operation for a non-existent target)
				}
			} else {
				// specific whiteout: delete target file/directory
				targetPath := filepath.Join(dst, whTarget)
				if err := removeAllSafe(targetPath); err != nil {
					// log but don't fail - file may not exist
				}
			}
			continue // don't extract the .wh. file itself
		}

		target := filepath.Join(dst, filepath.Clean(name))

		switch h.Typeflag {
		case tar.TypeDir:
			// create directory
			if err := os.MkdirAll(target, os.FileMode(h.Mode)); err != nil {
				return err
			}

		case tar.TypeReg, tar.TypeRegA:
			// regular file with size tracking
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}

			out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(h.Mode))
			if err != nil {
				return err
			}

			// copy with size limit to prevent zip bomb expansion
			remaining := maxExtractedSize - totalExtracted
			if remaining <= 0 {
				out.Close()
				return fmt.Errorf("extracted size limit exceeded: %d bytes", maxExtractedSize)
			}

			written, err := io.CopyN(out, tr, remaining)
			totalExtracted += written

			// check if we hit the limit or if there's more data
			if err != nil && !errors.Is(err, io.EOF) {
				out.Close()
				return fmt.Errorf("extraction error: %w", err)
			}

			// verify we copied the expected amount
			if written < h.Size && !errors.Is(err, io.EOF) {
				out.Close()
				return fmt.Errorf("incomplete file extraction: %s", name)
			}

			out.Close()

		case tar.TypeSymlink:
			// validate symlink target and create if safe
			linkTarget := h.Linkname

			// reject absolute symlinks
			if filepath.IsAbs(linkTarget) {
				continue
			}

			// reject symlinks pointing outside extraction directory
			resolved := filepath.Join(filepath.Dir(target), linkTarget)
			cleanResolved := filepath.Clean(resolved)
			if !strings.HasPrefix(cleanResolved, filepath.Clean(dst)) {
				continue
			}

			// create symlink (relative paths only, validated above)
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			_ = os.Remove(target) // remove if exists
			if err := os.Symlink(linkTarget, target); err != nil {
				// log but don't fail - symlink may be to non-existent target
			}

		case tar.TypeLink:
			// hardlinks: validate target is within extraction directory
			linkTarget := h.Linkname

			if isBadPath(linkTarget) {
				continue // skip dangerous hardlinks
			}

			// resolve target path
			targetPath := filepath.Join(dst, filepath.Clean(linkTarget))

			// ensure target is within extraction directory
			if !strings.HasPrefix(targetPath, filepath.Clean(dst)) {
				continue
			}

			// create hardlink if target exists
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			_ = os.Remove(target) // remove if exists
			if err := os.Link(targetPath, target); err != nil {
				// log but don't fail - target may not exist yet
			}

		default:
			// skip other types (devices, fifos, etc.)
			continue
		}
	}

	return nil
}

// removeallsafe removes a file or directory, ignoring "not exist" errors.
// used for oci whiteout file processing.
func removeAllSafe(path string) error {
	err := os.RemoveAll(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
