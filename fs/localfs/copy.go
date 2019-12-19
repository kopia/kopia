package localfs

import (
	"context"
	"os"
	"path/filepath"

	"github.com/natefinch/atomic"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

// Copy copies e into targetPath in the local file system. If e is an
// fs.Directory, then the contents are recursively copied.
// The targetPath must not exist, except when the target path is the root
// directory. In that case, e must be a fs.Directory and its contents are copied
// to the root directory.
// Copy does not overwrite files or directories and returns an error in that
// case. It also returns an error when the the contents cannot be restored,
// for example due to an I/O error.
func Copy(ctx context.Context, targetPath string, e fs.Entry) error {
	targetPath, err := filepath.Abs(filepath.FromSlash(targetPath))
	if err != nil {
		return err
	}

	return copyEntry(ctx, e, targetPath)
}

func copyEntry(ctx context.Context, e fs.Entry, targetPath string) error {
	var err error

	switch e := e.(type) {
	case fs.Directory:
		err = copyDirectory(ctx, e, targetPath)
	case fs.File:
		err = copyFileContent(ctx, targetPath, e)
	case fs.Symlink:
		// Not yet implemented
		log.Warningf("Not creating symlink %q from %v", targetPath, e)
		return nil
	default:
		return errors.Errorf("invalid FS entry type for %q: %#v", targetPath, e)
	}

	if err != nil {
		return err
	}

	return setAttributes(targetPath, e)
}

// set permission, modification time and user/group ids on targetPath
func setAttributes(targetPath string, e fs.Entry) error {
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

func copyDirectory(ctx context.Context, d fs.Directory, targetPath string) error {
	if err := createDirectory(targetPath); err != nil {
		return err
	}

	return copyDirectoryContent(ctx, d, targetPath)
}

func copyDirectoryContent(ctx context.Context, d fs.Directory, targetPath string) error {
	entries, err := d.Readdir(ctx)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if err := copyEntry(ctx, e, filepath.Join(targetPath, e.Name())); err != nil {
			return err
		}
	}

	return nil
}

func createDirectory(path string) error {
	switch stat, err := os.Stat(path); {
	case os.IsNotExist(err):
		return os.MkdirAll(path, 0700)
	case err != nil:
		return errors.Wrap(err, "failed to stat path "+path)
	case stat.Mode().IsDir():
		return errors.Errorf("directory already exists, not overwriting it: %q", path)
	default:
		return errors.Errorf("unable to create directory, %q already exists and it is not a directory", path)
	}
}

func copyFileContent(ctx context.Context, targetPath string, f fs.File) error {
	switch _, err := os.Stat(targetPath); {
	case os.IsNotExist(err): // copy file below
	case err == nil:
		return errors.Errorf("unable to create %q, it already exists", targetPath)
	default:
		return errors.Wrap(err, "failed to stat "+targetPath)
	}

	r, err := f.Open(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to open snapshot file for "+targetPath)
	}
	defer r.Close() //nolint:errcheck

	return atomic.WriteFile(targetPath, r)
}
