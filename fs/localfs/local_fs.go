package localfs

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/pagecache"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("kopia/localfs")

const numEntriesToRead = 100 // number of directory entries to read in one shot

// Options configures behavior of a local filesystem entry tree created by
// NewEntryWithOptions. The zero value means default behavior.
type Options struct {
	// StreamingReads indicates whether to hint the OS regarding how to cache
	// files opened through this entry (tree). The option is propagated to all
	// the children in a subtree.
	// HintStreaming is issued at Open() and HintNotNeeded at Close().
	// These hints are advisory-only and only effective on Linux.
	StreamingReads bool
}

type filesystemEntry struct {
	name       string
	size       int64
	mtimeNanos int64
	mode       os.FileMode
	owner      fs.OwnerInfo
	device     fs.DeviceInfo

	prefix string
	opts   Options
}

func (e *filesystemEntry) Name() string {
	return e.name
}

func (e *filesystemEntry) IsDir() bool {
	return e.mode.IsDir()
}

func (e *filesystemEntry) Mode() os.FileMode {
	return e.mode
}

func (e *filesystemEntry) Size() int64 {
	return e.size
}

func (e *filesystemEntry) ModTime() time.Time {
	return time.Unix(0, e.mtimeNanos)
}

func (e *filesystemEntry) Sys() any {
	return nil
}

func (e *filesystemEntry) fullPath() string {
	return e.prefix + e.Name()
}

func (e *filesystemEntry) Owner() fs.OwnerInfo {
	return e.owner
}

func (e *filesystemEntry) Device() fs.DeviceInfo {
	return e.device
}

func (e *filesystemEntry) LocalFilesystemPath() string {
	return e.fullPath()
}

type filesystemDirectory struct {
	filesystemEntry
}

type filesystemSymlink struct {
	filesystemEntry
}

type filesystemFile struct {
	filesystemEntry
}

type filesystemErrorEntry struct {
	filesystemEntry
	err error
}

func (fsd *filesystemDirectory) SupportsMultipleIterations() bool {
	return true
}

func (fsd *filesystemDirectory) Size() int64 {
	// force directory size to always be zero
	return 0
}

type fileWithMetadata struct {
	*os.File

	// opts may be used for operations on the file, including Close().
	opts Options
}

func (f *fileWithMetadata) Entry() (fs.Entry, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "unable to stat() local file")
	}

	basename, prefix := splitDirPrefix(f.Name())

	return newFilesystemFile(newEntry(basename, fi, prefix, f.opts)), nil
}

func (f *fileWithMetadata) Close() error {
	if f.opts.StreamingReads {
		// HintNotNeeded is advisory and best-effort; failures don't affect
		// correctness. The error is intentionally ignored and not logged given
		// that Close() has no ctx to retrieve a ctx-derived logger.
		_ = pagecache.HintNotNeeded(f.File)
	}

	if err := f.File.Close(); err != nil {
		return errors.Wrap(err, "unable to close local file")
	}

	return nil
}

func (fsf *filesystemFile) Open(ctx context.Context) (fs.Reader, error) {
	f, err := os.Open(fsf.fullPath())
	if err != nil {
		return nil, errors.Wrap(err, "unable to open local file")
	}

	// In streaming-reads mode, hint the kernel for readahead at open
	// (HintStreaming) and to drop the pages at close (HintNotNeeded).
	if fsf.opts.StreamingReads {
		if hintErr := pagecache.HintStreaming(f); hintErr != nil {
			log(ctx).Debugf("page cache hint at open failed for %q: %v", f.Name(), hintErr)
		}
	}

	return &fileWithMetadata{File: f, opts: fsf.opts}, nil
}

func (fsl *filesystemSymlink) Readlink(_ context.Context) (string, error) {
	//nolint:wrapcheck
	return os.Readlink(fsl.fullPath())
}

func (fsl *filesystemSymlink) Resolve(_ context.Context) (fs.Entry, error) {
	target, err := filepath.EvalSymlinks(fsl.fullPath())
	if err != nil {
		return nil, errors.Wrapf(err, "cannot resolve symlink for '%q'", fsl.fullPath())
	}

	return NewEntryWithOptions(target, fsl.opts)
}

func (e *filesystemErrorEntry) ErrorInfo() error {
	return e.err
}

// splitDirPrefix returns the directory prefix for a given path - the initial part of the path up to and including the final slash (or backslash on Windows).
// this is similar to filepath.Dir() and filepath.Base() except splitDirPrefix("\\foo\bar") == "\\foo\", which is unsupported in filepath.
func splitDirPrefix(s string) (basename, prefix string) {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == filepath.Separator || s[i] == '/' {
			return s[i+1:], s[0 : i+1]
		}
	}

	return s, ""
}

// Directory returns fs.Directory for the specified path with default Options.
func Directory(path string) (fs.Directory, error) {
	return directoryWithOptions(path, Options{})
}

// directoryWithOptions configures the returned directory (and its descendants) with opts.
func directoryWithOptions(path string, opts Options) (fs.Directory, error) {
	e, err := NewEntryWithOptions(path, opts)
	if err != nil {
		return nil, err
	}

	switch e := e.(type) {
	case *filesystemDirectory:
		return e, nil

	case *filesystemSymlink:
		// it's a symbolic link, possibly to a directory, it may work or we may get a ReadDir() error.
		// this is apparently how VSS mounted snapshots appear on Windows and attempts to os.Readlink() fail on them.
		return newFilesystemDirectory(e.filesystemEntry), nil

	default:
		return nil, errors.Errorf("not a directory: %v (was %T)", path, e)
	}
}

var (
	_ fs.Directory  = (*filesystemDirectory)(nil)
	_ fs.File       = (*filesystemFile)(nil)
	_ fs.Symlink    = (*filesystemSymlink)(nil)
	_ fs.ErrorEntry = (*filesystemErrorEntry)(nil)
)
