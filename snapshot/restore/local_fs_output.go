// Package restore manages restoring filesystem snapshots.
package restore

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/natefinch/atomic"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
)

// FilesystemOutput contains the options for outputting a file system tree.
type FilesystemOutput struct {
	// TargetPath for restore.
	TargetPath string

	// If a directory already exists, overwrite the directory.
	OverwriteDirectories bool

	// Indicate whether or not to overwrite existing files. When set to false,
	// the copier does not modify already existing files and returns an error
	// instead.
	OverwriteFiles bool
}

// BeginDirectory implements restore.Output interface.
func (o *FilesystemOutput) BeginDirectory(ctx context.Context, relativePath string, e fs.Directory) error {
	path := filepath.Join(o.TargetPath, filepath.FromSlash(relativePath))

	if err := o.createDirectory(ctx, path); err != nil {
		return errors.Wrap(err, "error creating directory")
	}

	return nil
}

// FinishDirectory implements restore.Output interface.
func (o *FilesystemOutput) FinishDirectory(ctx context.Context, relativePath string, e fs.Directory) error {
	path := filepath.Join(o.TargetPath, filepath.FromSlash(relativePath))
	if err := o.setAttributes(path, e); err != nil {
		return errors.Wrap(err, "error setting attributes")
	}

	return nil
}

// Close implements restore.Output interface.
func (o *FilesystemOutput) Close(ctx context.Context) error {
	return nil
}

// WriteFile implements restore.Output interface.
func (o *FilesystemOutput) WriteFile(ctx context.Context, relativePath string, f fs.File) error {
	log(ctx).Infof("WriteFile %v %v", relativePath, f)
	path := filepath.Join(o.TargetPath, filepath.FromSlash(relativePath))

	if err := o.copyFileContent(ctx, path, f); err != nil {
		return errors.Wrap(err, "error creating directory")
	}

	if err := o.setAttributes(path, f); err != nil {
		return errors.Wrap(err, "error setting attributes")
	}

	return nil
}

// CreateSymlink implements restore.Output interface.
func (o *FilesystemOutput) CreateSymlink(ctx context.Context, relativePath string, e fs.Symlink) error {
	log(ctx).Debugf("create symlink not implemented yet")
	return nil
}

// set permission, modification time and user/group ids on targetPath.
func (o *FilesystemOutput) setAttributes(targetPath string, e fs.Entry) error {
	const modBits = os.ModePerm | os.ModeSetgid | os.ModeSetuid | os.ModeSticky

	le, err := localfs.NewEntry(targetPath)
	if err != nil {
		return errors.Wrap(err, "could not create local FS entry for "+targetPath)
	}

	// Set owner user and group from e
	// On Windows Chown is not supported. fs.OwnerInfo collected on Windows will always
	// be zero-value for UID and GID, so the Chown operation is not performed.
	if le.Owner() != e.Owner() && runtime.GOOS != "windows" {
		if err = os.Chown(targetPath, int(e.Owner().UserID), int(e.Owner().GroupID)); err != nil && !os.IsPermission(err) {
			return errors.Wrap(err, "could not change owner/group for "+targetPath)
		}
	}

	// Set file permissions from e
	if (le.Mode() & modBits) != (e.Mode() & modBits) {
		if err = os.Chmod(targetPath, e.Mode()&modBits); err != nil && !os.IsPermission(err) {
			return errors.Wrap(err, "could not change permissions on "+targetPath)
		}
	}

	// Set mod time from e
	if !le.ModTime().Equal(e.ModTime()) {
		// Note: Set atime to ModTime as well
		if err = os.Chtimes(targetPath, e.ModTime(), e.ModTime()); err != nil && !os.IsPermission(err) {
			return errors.Wrap(err, "could not change mod time on "+targetPath)
		}
	}

	return nil
}

func (o *FilesystemOutput) createDirectory(ctx context.Context, path string) error {
	switch stat, err := os.Stat(path); {
	case os.IsNotExist(err):
		return os.MkdirAll(path, 0o700)
	case err != nil:
		return errors.Wrap(err, "failed to stat path "+path)
	case stat.Mode().IsDir():
		if !o.OverwriteDirectories {
			if empty, _ := isEmptyDirectory(path); !empty {
				return errors.Errorf("non-empty directory already exists, not overwriting it: %q", path)
			}
		}

		log(ctx).Debugf("Not creating already existing directory: %v", path)

		return nil
	default:
		return errors.Errorf("unable to create directory, %q already exists and it is not a directory", path)
	}
}

func (o *FilesystemOutput) copyFileContent(ctx context.Context, targetPath string, f fs.File) error {
	switch _, err := os.Stat(targetPath); {
	case os.IsNotExist(err): // copy file below
	case err == nil:
		if !o.OverwriteFiles {
			return errors.Errorf("unable to create %q, it already exists", targetPath)
		}

		log(ctx).Debugf("Overwriting existing file: %v", targetPath)
	default:
		return errors.Wrap(err, "failed to stat "+targetPath)
	}

	r, err := f.Open(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to open snapshot file for "+targetPath)
	}
	defer r.Close() //nolint:errcheck

	log(ctx).Debugf("copying file contents to: %v", targetPath)

	return atomic.WriteFile(targetPath, r)
}

func isEmptyDirectory(name string) (bool, error) {
	f, err := os.Open(name) //nolint:gosec
	if err != nil {
		return false, err
	}

	defer f.Close() //nolint:errcheck,gosec

	if _, err = f.Readdirnames(1); err == io.EOF {
		return true, nil
	}

	return false, err // Either not empty or error
}
