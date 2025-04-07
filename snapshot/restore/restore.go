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

// FileWriteProgress is a callback used to report amount of data sent to the output.
type FileWriteProgress func(chunkSize int64)

// Output encapsulates output for restore operation.
type Output interface {
	Parallelizable() bool
	BeginDirectory(ctx context.Context, relativePath string, e fs.Directory) error
	WriteDirEntry(ctx context.Context, relativePath string, de *snapshot.DirEntry, e fs.Directory) error
	FinishDirectory(ctx context.Context, relativePath string, e fs.Directory) error
	WriteFile(ctx context.Context, relativePath string, e fs.File, progressCb FileWriteProgress) error
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

// ProgressCallback is a callback used to report progress of snapshot restore.
type ProgressCallback func(ctx context.Context, s Stats)

// Options provides optional restore parameters.
type Options struct {
	// NOTE: this structure is passed as-is from the UI, make sure to add
	// required bindings in the UI.
	Parallel               int   `json:"parallel"`
	Incremental            bool  `json:"incremental"`
	IgnoreErrors           bool  `json:"ignoreErrors"`
	RestoreDirEntryAtDepth int32 `json:"restoreDirEntryAtDepth"`
	MinSizeForPlaceholder  int32 `json:"minSizeForPlaceholder"`

	ProgressCallback ProgressCallback `json:"-"`
	Cancel           chan struct{}    `json:"-"` // channel that can be externally closed to signal cancellation
}

// Entry walks a snapshot root with given root entry and restores it to the provided output.
//
//nolint:revive
func Entry(ctx context.Context, rep repo.Repository, output Output, rootEntry fs.Entry, options Options) (Stats, error) {
	c := copier{
		output:           output,
		shallowoutput:    makeShallowFilesystemOutput(output, options),
		q:                parallelwork.NewQueue(),
		incremental:      options.Incremental,
		ignoreErrors:     options.IgnoreErrors,
		cancel:           options.Cancel,
		progressCallback: options.ProgressCallback,
	}

	c.q.ProgressCallback = func(ctx context.Context, enqueued, active, completed int64) {
		c.reportProgress(ctx)
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
	ignoreErrors  bool
	cancel        chan struct{}

	progressCallback ProgressCallback
}

func (c *copier) reportProgress(ctx context.Context) {
	if c.progressCallback != nil {
		c.progressCallback(ctx, c.stats.clone())
	}
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

		bytesExpected := e.Size()
		bytesWritten := int64(0)
		progressCallback := func(chunkSize int64) {
			bytesWritten += chunkSize
			c.stats.RestoredTotalFileSize.Add(chunkSize)
			c.reportProgress(ctx)
		}

		if currentdepth > maxdepth {
			if err := c.shallowoutput.WriteFile(ctx, targetPath, e, progressCallback); err != nil {
				return errors.Wrap(err, "copy file")
			}
		} else {
			if err := c.output.WriteFile(ctx, targetPath, e, progressCallback); err != nil {
				return errors.Wrap(err, "copy file")
			}
		}

		c.stats.RestoredFileCount.Add(1)
		c.stats.RestoredTotalFileSize.Add(bytesExpected - bytesWritten)

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
			return errors.Errorf("fs.Directory '%s' object is not HasDirEntry?", d.Name())
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
