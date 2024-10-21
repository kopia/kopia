package localfs

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

const numEntriesToRead = 100 // number of directory entries to read in one shot

type filesystemEntry struct {
	name       string
	size       int64
	mtimeNanos int64
	mode       os.FileMode
	owner      fs.OwnerInfo
	device     fs.DeviceInfo

	prefix string
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

func (e *filesystemEntry) Sys() interface{} {
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
}

func (f *fileWithMetadata) Entry() (fs.Entry, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "unable to stat() local file")
	}

	return newFilesystemFile(newEntry(fi, dirPrefix(f.Name()))), nil
}

func (fsf *filesystemFile) Open(ctx context.Context) (fs.Reader, error) {
	f, err := os.Open(fsf.fullPath())
	if err != nil {
		return nil, errors.Wrap(err, "unable to open local file")
	}

	return &fileWithMetadata{f}, nil
}

func (fsl *filesystemSymlink) Readlink(ctx context.Context) (string, error) {
	//nolint:wrapcheck
	return os.Readlink(fsl.fullPath())
}

func (fsl *filesystemSymlink) Resolve(ctx context.Context) (fs.Entry, error) {
	target, err := filepath.EvalSymlinks(fsl.fullPath())
	if err != nil {
		return nil, errors.Wrapf(err, "while reading symlink %s", fsl.fullPath())
	}

	entry, err := NewEntry(target)

	return entry, err
}

func (e *filesystemErrorEntry) ErrorInfo() error {
	return e.err
}

// dirPrefix returns the directory prefix for a given path - the initial part of the path up to and including the final slash (or backslash on Windows).
// this is similar to filepath.Dir() except dirPrefix("\\foo\bar") == "\\foo\", which is unsupported in filepath.
func dirPrefix(s string) string {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == filepath.Separator || s[i] == '/' {
			return s[0 : i+1]
		}
	}

	return ""
}

// Directory returns fs.Directory for the specified path.
func Directory(path string) (fs.Directory, error) {
	e, err := NewEntry(path)
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
