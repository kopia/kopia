package localfs

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/natefinch/atomic"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

// CopyOptions contains the options for copying a file system tree
type CopyOptions struct {
	// If a directory already exists, overwrite the directory.
	OverwriteDirectories bool
	// Indicate whether or not to overwrite existing files. When set to false,
	// the copier does not modify already existing files and returns an error
	// instead.
	OverwriteFiles bool
}

// Copy copies e into targetPath in the local file system. If e is an
// fs.Directory, then the contents are recursively copied.
// The targetPath must not exist, except when the target path is the root
// directory. In that case, e must be a fs.Directory and its contents are copied
// to the root directory.
// Copy does not overwrite files or directories and returns an error in that
// case. It also returns an error when the the contents cannot be restored,
// for example due to an I/O error.
func Copy(ctx context.Context, targetPath string, e fs.Entry, opt CopyOptions) error {
	targetPath, err := filepath.Abs(filepath.FromSlash(targetPath))
	if err != nil {
		return err
	}

	c := copier{CopyOptions: opt}

	return c.copyEntry(ctx, e, targetPath)
}

type copier struct {
	CopyOptions
}

func (c *copier) copyEntry(ctx context.Context, e fs.Entry, targetPath string) error {
	var err error

	switch e := e.(type) {
	case fs.Directory:
		err = c.copyDirectory(ctx, e, targetPath)
	case fs.File:
		err = c.copyFileContent(ctx, targetPath, e)
	case fs.Symlink:
		// Not yet implemented
		log(ctx).Warningf("Not creating symlink %q from %v", targetPath, e)
		return nil
	default:
		return errors.Errorf("invalid FS entry type for %q: %#v", targetPath, e)
	}

	if err != nil {
		return err
	}

	return c.setAttributes(targetPath, e)
}

// set permission, modification time and user/group ids on targetPath
func (c *copier) setAttributes(targetPath string, e fs.Entry) error {
	const modBits = os.ModePerm | os.ModeSetgid | os.ModeSetuid | os.ModeSticky

	le, err := NewEntry(targetPath)
	if err != nil {
		return errors.Wrap(err, "could not create local FS entry for "+targetPath)
	}

	// Set owner user and group from e
	if le.Owner() != e.Owner() {
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

func (c *copier) copyDirectory(ctx context.Context, d fs.Directory, targetPath string) error {
	if err := c.createDirectory(ctx, targetPath); err != nil {
		return err
	}

	return c.copyDirectoryContent(ctx, d, targetPath)
}

func (c *copier) copyDirectoryContent(ctx context.Context, d fs.Directory, targetPath string) error {
	entries, err := d.Readdir(ctx)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if err := c.copyEntry(ctx, e, filepath.Join(targetPath, e.Name())); err != nil {
			return err
		}
	}

	return nil
}

func (c *copier) createDirectory(ctx context.Context, path string) error {
	switch stat, err := os.Stat(path); {
	case os.IsNotExist(err):
		return os.MkdirAll(path, 0700)
	case err != nil:
		return errors.Wrap(err, "failed to stat path "+path)
	case stat.Mode().IsDir():
		if !c.OverwriteDirectories {
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

func (c *copier) copyFileContent(ctx context.Context, targetPath string, f fs.File) error {
	switch _, err := os.Stat(targetPath); {
	case os.IsNotExist(err): // copy file below
	case err == nil:
		if !c.OverwriteFiles {
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

	defer f.Close() //nolint:errcheck

	if _, err = f.Readdirnames(1); err == io.EOF {
		return true, nil
	}

	return false, err // Either not empty or error
}
