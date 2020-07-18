package restore

import (
	"context"
	"path"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var log = logging.GetContextLoggerFunc("restore")

// Output encapsulates output for restore operation.
type Output interface {
	BeginDirectory(ctx context.Context, relativePath string, e fs.Directory) error
	FinishDirectory(ctx context.Context, relativePath string, e fs.Directory) error
	WriteFile(ctx context.Context, relativePath string, e fs.File) error
	CreateSymlink(ctx context.Context, relativePath string, e fs.Symlink) error
	Close(ctx context.Context) error
}

// Stats represents restore statistics.
type Stats struct {
	TotalFileSize int64
	FileCount     int32
	DirCount      int32
}

// Snapshot walks a snapshot root with given snapshot ID and restores it to the provided output.
func Snapshot(ctx context.Context, rep repo.Repository, output Output, snapID manifest.ID) (Stats, error) {
	m, err := snapshot.LoadSnapshot(ctx, rep, snapID)
	if err != nil {
		return Stats{}, err
	}

	if m.RootEntry == nil {
		return Stats{}, errors.Errorf("No root entry found in manifest (%v)", snapID)
	}

	rootEntry, err := snapshotfs.SnapshotRoot(rep, m)
	if err != nil {
		return Stats{}, err
	}

	return copyToOutput(ctx, output, rootEntry)
}

// Root walks a snapshot root with given object ID and restores it to the provided output.
func Root(ctx context.Context, rep repo.Repository, output Output, oid object.ID) (Stats, error) {
	return copyToOutput(ctx, output, snapshotfs.DirectoryEntry(rep, oid, nil))
}

func copyToOutput(ctx context.Context, output Output, rootEntry fs.Entry) (Stats, error) {
	c := copier{output: output}

	if err := c.copyEntry(ctx, rootEntry, ""); err != nil {
		return Stats{}, errors.Wrap(err, "error copying")
	}

	if err := c.output.Close(ctx); err != nil {
		return Stats{}, errors.Wrap(err, "error closing output")
	}

	return c.stats, nil
}

type copier struct {
	stats  Stats
	output Output
}

func (c *copier) copyEntry(ctx context.Context, e fs.Entry, targetPath string) error {
	switch e := e.(type) {
	case fs.Directory:
		log(ctx).Debugf("dir: '%v'", targetPath)
		return c.copyDirectory(ctx, e, targetPath)
	case fs.File:
		log(ctx).Debugf("file: '%v'", targetPath)

		atomic.AddInt32(&c.stats.FileCount, 1)
		atomic.AddInt64(&c.stats.TotalFileSize, e.Size())

		return c.output.WriteFile(ctx, targetPath, e)
	case fs.Symlink:
		log(ctx).Debugf("symlink: '%v'", targetPath)
		return c.output.CreateSymlink(ctx, targetPath, e)
	default:
		return errors.Errorf("invalid FS entry type for %q: %#v", targetPath, e)
	}
}

func (c *copier) copyDirectory(ctx context.Context, d fs.Directory, targetPath string) error {
	atomic.AddInt32(&c.stats.DirCount, 1)

	if err := c.output.BeginDirectory(ctx, targetPath, d); err != nil {
		return errors.Wrap(err, "create directory")
	}

	if err := c.copyDirectoryContent(ctx, d, targetPath); err != nil {
		return errors.Wrap(err, "copy directory contents")
	}

	if err := c.output.FinishDirectory(ctx, targetPath, d); err != nil {
		return errors.Wrap(err, "finish directory")
	}

	return nil
}

func (c *copier) copyDirectoryContent(ctx context.Context, d fs.Directory, targetPath string) error {
	entries, err := d.Readdir(ctx)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if err := c.copyEntry(ctx, e, path.Join(targetPath, e.Name())); err != nil {
			return err
		}
	}

	return nil
}
