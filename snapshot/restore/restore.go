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
)

var log = logging.GetContextLoggerFunc("restore")

// Output encapsulates output for restore operation.
type Output interface {
	Parallelizable() bool
	BeginDirectory(ctx context.Context, relativePath string, e fs.Directory) error
	FinishDirectory(ctx context.Context, relativePath string, e fs.Directory) error
	WriteFile(ctx context.Context, relativePath string, e fs.File) error
	CreateSymlink(ctx context.Context, relativePath string, e fs.Symlink) error
	Close(ctx context.Context) error
}

// Stats represents restore statistics.
type Stats struct {
	RestoredTotalFileSize int64
	EnqueuedTotalFileSize int64

	RestoredFileCount    int32
	RestoredDirCount     int32
	RestoredSymlinkCount int32
	EnqueuedFileCount    int32
	EnqueuedDirCount     int32
	EnqueuedSymlinkCount int32
}

func (s *Stats) clone() Stats {
	return Stats{
		RestoredTotalFileSize: atomic.LoadInt64(&s.RestoredTotalFileSize),
		EnqueuedTotalFileSize: atomic.LoadInt64(&s.EnqueuedTotalFileSize),
		RestoredFileCount:     atomic.LoadInt32(&s.RestoredFileCount),
		RestoredDirCount:      atomic.LoadInt32(&s.RestoredDirCount),
		RestoredSymlinkCount:  atomic.LoadInt32(&s.RestoredSymlinkCount),
		EnqueuedFileCount:     atomic.LoadInt32(&s.EnqueuedFileCount),
		EnqueuedDirCount:      atomic.LoadInt32(&s.EnqueuedDirCount),
		EnqueuedSymlinkCount:  atomic.LoadInt32(&s.EnqueuedSymlinkCount),
	}
}

// Options provides optional restore parameters.
type Options struct {
	Parallel         int
	ProgressCallback func(ctx context.Context, s Stats)
}

// Entry walks a snapshot root with given root entry and restores it to the provided output.
func Entry(ctx context.Context, rep repo.Repository, output Output, rootEntry fs.Entry, options Options) (Stats, error) {
	c := copier{output: output, q: parallelwork.NewQueue()}

	c.q.ProgressCallback = func(ctx context.Context, enqueued, active, completed int64) {
		options.ProgressCallback(ctx, c.stats.clone())
	}

	c.q.EnqueueFront(ctx, func() error {
		return errors.Wrap(c.copyEntry(ctx, rootEntry, "", func() error { return nil }), "error copying")
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
	stats  Stats
	output Output
	q      *parallelwork.Queue
}

func (c *copier) copyEntry(ctx context.Context, e fs.Entry, targetPath string, onCompletion func() error) error {
	switch e := e.(type) {
	case fs.Directory:
		log(ctx).Debugf("dir: '%v'", targetPath)
		return c.copyDirectory(ctx, e, targetPath, onCompletion)
	case fs.File:
		log(ctx).Debugf("file: '%v'", targetPath)

		atomic.AddInt32(&c.stats.RestoredFileCount, 1)
		atomic.AddInt64(&c.stats.RestoredTotalFileSize, e.Size())

		if err := c.output.WriteFile(ctx, targetPath, e); err != nil {
			return errors.Wrap(err, "copy file")
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

func (c *copier) copyDirectory(ctx context.Context, d fs.Directory, targetPath string, onCompletion parallelwork.CallbackFunc) error {
	atomic.AddInt32(&c.stats.RestoredDirCount, 1)

	if err := c.output.BeginDirectory(ctx, targetPath, d); err != nil {
		return errors.Wrap(err, "create directory")
	}

	return errors.Wrap(c.copyDirectoryContent(ctx, d, targetPath, func() error {
		if err := c.output.FinishDirectory(ctx, targetPath, d); err != nil {
			return errors.Wrap(err, "finish directory")
		}

		return onCompletion()
	}), "copy directory contents")
}

func (c *copier) copyDirectoryContent(ctx context.Context, d fs.Directory, targetPath string, onCompletion parallelwork.CallbackFunc) error {
	entries, err := d.Readdir(ctx)
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
				return c.copyEntry(ctx, e, path.Join(targetPath, e.Name()), onItemCompletion)
			})
		} else {
			if isSymlink(e) {
				atomic.AddInt32(&c.stats.EnqueuedSymlinkCount, 1)
			} else {
				atomic.AddInt32(&c.stats.EnqueuedFileCount, 1)
			}

			atomic.AddInt64(&c.stats.EnqueuedTotalFileSize, e.Size())

			c.q.EnqueueBack(ctx, func() error {
				return c.copyEntry(ctx, e, path.Join(targetPath, e.Name()), onItemCompletion)
			})
		}
	}

	return nil
}
