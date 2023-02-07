package restore

import (
	"context"
	"os"
	"path"
	"runtime"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/parallelwork"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/snapshot"
)

var log = logging.Module("restore")

// Output encapsulates output for restore operation.
type Output interface {
	Parallelizable() bool
	BeginDirectory(ctx context.Context, relativePath string, e fs.Directory) error
	WriteDirEntry(ctx context.Context, relativePath string, de *snapshot.DirEntry, e fs.Directory) error
	FinishDirectory(ctx context.Context, relativePath string, e fs.Directory) error
	WriteFile(ctx context.Context, relativePath string, e fs.File) error
	FileExists(ctx context.Context, relativePath string, e fs.File) bool
	CreateSymlink(ctx context.Context, relativePath string, e fs.Symlink) error
	SymlinkExists(ctx context.Context, relativePath string, e fs.Symlink) bool
	Close(ctx context.Context) error
}

// Stats represents restore statistics.
type Stats struct {
	RestoredTotalFileSize int64
	EnqueuedTotalFileSize int64
	SkippedTotalFileSize  int64

	RestoredFileCount    int32
	RestoredDirCount     int32
	RestoredSymlinkCount int32
	EnqueuedFileCount    int32
	EnqueuedDirCount     int32
	EnqueuedSymlinkCount int32
	SkippedCount         int32
	IgnoredErrorCount    int32
}

// stats represents restore statistics.
type statsInternal struct {
	RestoredTotalFileSize atomic.Int64
	EnqueuedTotalFileSize atomic.Int64
	SkippedTotalFileSize  atomic.Int64

	RestoredFileCount    atomic.Int32
	RestoredDirCount     atomic.Int32
	RestoredSymlinkCount atomic.Int32
	EnqueuedFileCount    atomic.Int32
	EnqueuedDirCount     atomic.Int32
	EnqueuedSymlinkCount atomic.Int32
	SkippedCount         atomic.Int32
	IgnoredErrorCount    atomic.Int32
}

func (s *statsInternal) clone() Stats {
	return Stats{
		RestoredTotalFileSize: s.RestoredTotalFileSize.Load(),
		EnqueuedTotalFileSize: s.EnqueuedTotalFileSize.Load(),
		SkippedTotalFileSize:  s.SkippedTotalFileSize.Load(),
		RestoredFileCount:     s.RestoredFileCount.Load(),
		RestoredDirCount:      s.RestoredDirCount.Load(),
		RestoredSymlinkCount:  s.RestoredSymlinkCount.Load(),
		EnqueuedFileCount:     s.EnqueuedFileCount.Load(),
		EnqueuedDirCount:      s.EnqueuedDirCount.Load(),
		EnqueuedSymlinkCount:  s.EnqueuedSymlinkCount.Load(),
		SkippedCount:          s.SkippedCount.Load(),
		IgnoredErrorCount:     s.IgnoredErrorCount.Load(),
	}
}

// Options provides optional restore parameters.
type Options struct {
	// NOTE: this structure is passed as-is from the UI, make sure to add
	// required bindings in the UI.
	Parallel               int   `json:"parallel"`
	Incremental            bool  `json:"incremental"`
	DeleteExtra            bool  `json:"deleteExtra"`
	IgnoreErrors           bool  `json:"ignoreErrors"`
	RestoreDirEntryAtDepth int32 `json:"restoreDirEntryAtDepth"`
	MinSizeForPlaceholder  int32 `json:"minSizeForPlaceholder"`

	ProgressCallback func(ctx context.Context, s Stats) `json:"-"`
	Cancel           chan struct{}                      `json:"-"` // channel that can be externally closed to signal cancellation
}

// Entry walks a snapshot root with given root entry and restores it to the provided output.
func Entry(ctx context.Context, rep repo.Repository, output Output, rootEntry fs.Entry, options Options) (Stats, error) {
	c := copier{
		output:        output,
		shallowoutput: makeShallowFilesystemOutput(output, options),
		q:             parallelwork.NewQueue(),
		incremental:   options.Incremental,
		deleteExtra:   options.DeleteExtra,
		ignoreErrors:  options.IgnoreErrors,
		cancel:        options.Cancel,
	}

	c.q.ProgressCallback = func(ctx context.Context, enqueued, active, completed int64) {
		if options.ProgressCallback != nil {
			options.ProgressCallback(ctx, c.stats.clone())
		}
	}

	// Control the depth of a restore. Default (options.MaxDepth = 0) is to restore to full depth.
	currentdepth := int32(0)

	c.q.EnqueueFront(ctx, func() error {
		return errors.Wrap(c.copyEntry(ctx, rootEntry, "", currentdepth, options.RestoreDirEntryAtDepth, func() error { return nil }), "error copying")
	})

	numWorkers := options.Parallel
	if numWorkers == 0 {
		numWorkers = runtime.NumCPU()
	}

	if !output.Parallelizable() {
		numWorkers = 1
	}

	if err := c.q.Process(ctx, numWorkers); err != nil {
		return Stats{}, errors.Wrap(err, "restore error")
	}

	if err := c.output.Close(ctx); err != nil {
		return Stats{}, errors.Wrap(err, "error closing output")
	}

	return c.stats.clone(), nil
}

type copier struct {
	stats         statsInternal
	output        Output
	shallowoutput Output
	q             *parallelwork.Queue
	incremental   bool
	deleteExtra   bool
	ignoreErrors  bool
	cancel        chan struct{}
}

func (c *copier) copyEntry(ctx context.Context, e fs.Entry, targetPath string, currentdepth, maxdepth int32, onCompletion func() error) error {
	if c.cancel != nil {
		select {
		case <-c.cancel:
			return onCompletion()

		default:
		}
	}

	if c.incremental {
		// in incremental mode, do not copy if the output already exists
		switch e := e.(type) {
		case fs.File:
			if c.output.FileExists(ctx, targetPath, e) {
				log(ctx).Debugf("skipping file %v because it already exists and metadata matches", targetPath)
				c.stats.SkippedCount.Add(1)
				c.stats.SkippedTotalFileSize.Add(e.Size())

				return onCompletion()
			}

		case fs.Symlink:
			if c.output.SymlinkExists(ctx, targetPath, e) {
				c.stats.SkippedCount.Add(1)
				log(ctx).Debugf("skipping symlink %v because it already exists", targetPath)

				return onCompletion()
			}
		}
	}

	err := c.copyEntryInternal(ctx, e, targetPath, currentdepth, maxdepth, onCompletion)
	if err == nil {
		return nil
	}

	if c.ignoreErrors {
		c.stats.IgnoredErrorCount.Add(1)
		log(ctx).Errorf("ignored error %v on %v", err, targetPath)

		return nil
	}

	return err
}

func (c *copier) copyEntryInternal(ctx context.Context, e fs.Entry, targetPath string, currentdepth, maxdepth int32, onCompletion func() error) error {
	switch e := e.(type) {
	case fs.Directory:
		log(ctx).Debugf("dir: '%v'", targetPath)
		return c.copyDirectory(ctx, e, targetPath, currentdepth, maxdepth, onCompletion)
	case fs.File:
		log(ctx).Debugf("file: '%v'", targetPath)

		c.stats.RestoredFileCount.Add(1)
		c.stats.RestoredTotalFileSize.Add(e.Size())

		if currentdepth > maxdepth {
			if err := c.shallowoutput.WriteFile(ctx, targetPath, e); err != nil {
				return errors.Wrap(err, "copy file")
			}
		} else {
			if err := c.output.WriteFile(ctx, targetPath, e); err != nil {
				return errors.Wrap(err, "copy file")
			}
		}

		return onCompletion()

	case fs.Symlink:
		c.stats.RestoredSymlinkCount.Add(1)
		log(ctx).Debugf("symlink: '%v'", targetPath)

		if err := c.output.CreateSymlink(ctx, targetPath, e); err != nil {
			return errors.Wrap(err, "create symlink")
		}

		return onCompletion()

	default:
		return errors.Errorf("invalid FS entry type for %q: %#v", targetPath, e)
	}
}

func (c *copier) copyDirectory(ctx context.Context, d fs.Directory, targetPath string, currentdepth, maxdepth int32, onCompletion parallelwork.CallbackFunc) error {
	c.stats.RestoredDirCount.Add(1)

	if SafelySuffixablePath(targetPath) && currentdepth > maxdepth {
		de, ok := d.(snapshot.HasDirEntry)
		if !ok {
			return errors.Errorf("fs.Directory object is not HasDirEntry?")
		}

		if err := c.shallowoutput.WriteDirEntry(ctx, targetPath, de.DirEntry(), d); err != nil {
			return errors.Wrap(err, "create directory")
		}

		return onCompletion()
	}

	if err := c.output.BeginDirectory(ctx, targetPath, d); err != nil {
		return errors.Wrap(err, "create directory")
	}

	// deleting existing files only makes sense in the context of an actual filesystem (compared to a tar or zip)
	_, isFileSystem := c.output.(*FilesystemOutput)
	_, isShallowFileSystem := c.output.(*ShallowFilesystemOutput)
	
	if c.deleteExtra && (isFileSystem || isShallowFileSystem) {
		if err := c.deleteExtraFilesInDir(ctx, d, targetPath); err != nil {
			return errors.Wrap(err, "delete extra")
		}
	}

	return errors.Wrap(c.copyDirectoryContent(ctx, d, targetPath, currentdepth+1, maxdepth, func() error {
		if err := c.output.FinishDirectory(ctx, targetPath, d); err != nil {
			return errors.Wrap(err, "finish directory")
		}

		return onCompletion()
	}), "copy directory contents")
}

func (c *copier) deleteExtraFilesInDir(ctx context.Context, d fs.Directory, targetPath string) error {
	entries, err := fs.GetAllEntries(ctx, d)
	if err != nil {
		return errors.Wrap(err, "error reading directory")
	}

	// first classify snapshot entries to help with deletion (treat symlinks like normal files)
	dirs := map[string]struct{}{}
	files := map[string]struct{}{}

	for _, e := range entries {
		if e.IsDir() {
			dirs[e.Name()] = struct{}{}
		} else /* file */ {
			files[e.Name()] = struct{}{}
		}
	}

	// read existing entries on disk
	existingEntries, err := os.ReadDir(targetPath)
	if err != nil {
		return errors.Wrap(err, "read existing dir entries")
	}

	// iterate existing entries, delete the ones that don't exist in the snapshot
	for _, e := range existingEntries {
		if e.IsDir() { //nolint:nestif
			_, existsInSnapshot := dirs[e.Name()]
			if !existsInSnapshot {
				if err := os.RemoveAll(path.Join(targetPath, e.Name())); err != nil {
					return errors.Wrap(err, "delete directory "+path.Join(targetPath, e.Name()))
				}
			}
		} else /* file */ {
			_, existsInSnapshot := files[e.Name()]
			if !existsInSnapshot {
				if err := os.Remove(path.Join(targetPath, e.Name())); err != nil {
					return errors.Wrap(err, "delete file "+path.Join(targetPath, e.Name()))
				}
			}
		}
	}

	return nil
}

func (c *copier) copyDirectoryContent(ctx context.Context, d fs.Directory, targetPath string, currentdepth, maxdepth int32, onCompletion parallelwork.CallbackFunc) error {
	entries, err := fs.GetAllEntries(ctx, d)
	if err != nil {
		return errors.Wrap(err, "error reading directory")
	}

	if len(entries) == 0 {
		return onCompletion()
	}

	onItemCompletion := parallelwork.OnNthCompletion(len(entries), onCompletion)

	for _, e := range entries {
		e := e

		if e.IsDir() {
			c.stats.EnqueuedDirCount.Add(1)
			// enqueue directories first, so that we quickly determine the total number and size of items.
			c.q.EnqueueFront(ctx, func() error {
				return c.copyEntry(ctx, e, path.Join(targetPath, e.Name()), currentdepth, maxdepth, onItemCompletion)
			})
		} else {
			if isSymlink(e) {
				c.stats.EnqueuedSymlinkCount.Add(1)
			} else {
				c.stats.EnqueuedFileCount.Add(1)
			}

			c.stats.EnqueuedTotalFileSize.Add(e.Size())

			c.q.EnqueueBack(ctx, func() error {
				return c.copyEntry(ctx, e, path.Join(targetPath, e.Name()), currentdepth, maxdepth, onItemCompletion)
			})
		}
	}

	return nil
}
