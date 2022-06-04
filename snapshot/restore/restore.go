package restore

import (
	"context"
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
	// +checkatomic
	RestoredTotalFileSize int64
	// +checkatomic
	EnqueuedTotalFileSize int64
	// +checkatomic
	SkippedTotalFileSize int64

	// +checkatomic
	RestoredFileCount int32
	// +checkatomic
	RestoredDirCount int32
	// +checkatomic
	RestoredSymlinkCount int32
	// +checkatomic
	EnqueuedFileCount int32
	// +checkatomic
	EnqueuedDirCount int32
	// +checkatomic
	EnqueuedSymlinkCount int32
	// +checkatomic
	SkippedCount int32
	// +checkatomic
	IgnoredErrorCount int32
}

func (s *Stats) clone() Stats {
	return Stats{
		RestoredTotalFileSize: atomic.LoadInt64(&s.RestoredTotalFileSize),
		EnqueuedTotalFileSize: atomic.LoadInt64(&s.EnqueuedTotalFileSize),
		SkippedTotalFileSize:  atomic.LoadInt64(&s.SkippedTotalFileSize),

		RestoredFileCount:    atomic.LoadInt32(&s.RestoredFileCount),
		RestoredDirCount:     atomic.LoadInt32(&s.RestoredDirCount),
		RestoredSymlinkCount: atomic.LoadInt32(&s.RestoredSymlinkCount),
		EnqueuedFileCount:    atomic.LoadInt32(&s.EnqueuedFileCount),
		EnqueuedDirCount:     atomic.LoadInt32(&s.EnqueuedDirCount),
		EnqueuedSymlinkCount: atomic.LoadInt32(&s.EnqueuedSymlinkCount),
		SkippedCount:         atomic.LoadInt32(&s.SkippedCount),
		IgnoredErrorCount:    atomic.LoadInt32(&s.IgnoredErrorCount),
	}
}

// Options provides optional restore parameters.
type Options struct {
	// NOTE: this structure is passed as-is from the UI, make sure to add
	// required bindings in the UI.
	Parallel               int   `json:"parallel"`
	Incremental            bool  `json:"incremental"`
	IgnoreErrors           bool  `json:"ignoreErrors"`
	RestoreDirEntryAtDepth int32 `json:"restoreDirEntryAtDepth"`
	MinSizeForPlaceholder  int32 `json:"minSizeForPlaceholder"`

	ProgressCallback func(ctx context.Context, s Stats)
	Cancel           chan struct{} // channel that can be externally closed to signal cancelation
}

// Entry walks a snapshot root with given root entry and restores it to the provided output.
func Entry(ctx context.Context, rep repo.Repository, output Output, rootEntry fs.Entry, options Options) (Stats, error) {
	c := copier{
		output:        output,
		shallowoutput: makeShallowFilesystemOutput(output, options),
		q:             parallelwork.NewQueue(),
		incremental:   options.Incremental,
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

	return c.stats, nil
}

type copier struct {
	stats         Stats
	output        Output
	shallowoutput Output
	q             *parallelwork.Queue
	incremental   bool
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
				atomic.AddInt32(&c.stats.SkippedCount, 1)
				atomic.AddInt64(&c.stats.SkippedTotalFileSize, e.Size())

				return onCompletion()
			}

		case fs.Symlink:
			if c.output.SymlinkExists(ctx, targetPath, e) {
				atomic.AddInt32(&c.stats.SkippedCount, 1)
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
		atomic.AddInt32(&c.stats.IgnoredErrorCount, 1)
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

		atomic.AddInt32(&c.stats.RestoredFileCount, 1)
		atomic.AddInt64(&c.stats.RestoredTotalFileSize, e.Size())

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
		atomic.AddInt32(&c.stats.RestoredSymlinkCount, 1)
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
	atomic.AddInt32(&c.stats.RestoredDirCount, 1)

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

	return errors.Wrap(c.copyDirectoryContent(ctx, d, targetPath, currentdepth+1, maxdepth, func() error {
		if err := c.output.FinishDirectory(ctx, targetPath, d); err != nil {
			return errors.Wrap(err, "finish directory")
		}

		return onCompletion()
	}), "copy directory contents")
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
			atomic.AddInt32(&c.stats.EnqueuedDirCount, 1)
			// enqueue directories first, so that we quickly determine the total number and size of items.
			c.q.EnqueueFront(ctx, func() error {
				return c.copyEntry(ctx, e, path.Join(targetPath, e.Name()), currentdepth, maxdepth, onItemCompletion)
			})
		} else {
			if isSymlink(e) {
				atomic.AddInt32(&c.stats.EnqueuedSymlinkCount, 1)
			} else {
				atomic.AddInt32(&c.stats.EnqueuedFileCount, 1)
			}

			atomic.AddInt64(&c.stats.EnqueuedTotalFileSize, e.Size())

			c.q.EnqueueBack(ctx, func() error {
				return c.copyEntry(ctx, e, path.Join(targetPath, e.Name()), currentdepth, maxdepth, onItemCompletion)
			})
		}
	}

	return nil
}
