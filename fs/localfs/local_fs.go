package localfs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

const (
	numEntriesToRead         = 100 // number of directory entries to read in one shot
	dirListingPrefetch       = 200 // number of directory items to os.Lstat() in advance
	paralellelStatGoroutines = 4   // how many goroutines to use when Lstat() on large directory
)

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

var _ os.FileInfo = (*filesystemEntry)(nil)

func newEntry(fi os.FileInfo, prefix string) filesystemEntry {
	return filesystemEntry{
		TrimShallowSuffix(fi.Name()),
		fi.Size(),
		fi.ModTime().UnixNano(),
		fi.Mode(),
		platformSpecificOwnerInfo(fi),
		platformSpecificDeviceInfo(fi),
		prefix,
	}
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

func (fsd *filesystemDirectory) Child(ctx context.Context, name string) (fs.Entry, error) {
	fullPath := fsd.fullPath()

	st, err := os.Lstat(filepath.Join(fullPath, name))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fs.ErrEntryNotFound
		}

		return nil, errors.Wrap(err, "unable to get child")
	}

	return entryFromDirEntry(st, fullPath+string(filepath.Separator)), nil
}

func toDirEntryOrNil(dirEntry os.DirEntry, prefix string) (fs.Entry, error) {
	fi, err := os.Lstat(prefix + dirEntry.Name())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, errors.Wrap(err, "error reading directory")
	}

	return entryFromDirEntry(fi, prefix), nil
}

type filesystemDirectoryIterator struct {
	dirHandle   *os.File
	childPrefix string

	currentIndex int
	currentBatch []os.DirEntry

	finalError error
}

func (it *filesystemDirectoryIterator) Next(ctx context.Context) fs.Entry {
	for {
		// we're at the end of the current batch, fetch the next batch
		if it.currentIndex >= len(it.currentBatch) {
			batch, err := it.dirHandle.ReadDir(numEntriesToRead)
			if err != nil && !errors.Is(err, io.EOF) {
				// stop iteration
				it.finalError = err
				return nil
			}

			it.currentIndex = 0
			it.currentBatch = batch

			// got empty batch
			if len(batch) == 0 {
				it.finalError = nil
				return nil
			}
		}

		n := it.currentIndex
		it.currentIndex++

		e, err := toDirEntryOrNil(it.currentBatch[n], it.childPrefix)
		if err != nil {
			// stop iteration
			it.finalError = err
			return nil
		}

		if e == nil {
			// go to the next item
			continue
		}

		return e
	}
}

func (it *filesystemDirectoryIterator) FinalErr() error {
	return it.finalError
}

func (it *filesystemDirectoryIterator) Close() {
	it.dirHandle.Close() //nolint:errcheck
}

func (fsd *filesystemDirectory) Iterate(ctx context.Context) (fs.DirectoryIterator, error) {
	fullPath := fsd.fullPath()

	f, direrr := os.Open(fullPath) //nolint:gosec
	if direrr != nil {
		return nil, errors.Wrap(direrr, "unable to read directory")
	}

	childPrefix := fullPath + string(filepath.Separator)

	return &filesystemDirectoryIterator{dirHandle: f, childPrefix: childPrefix}, nil
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

// NewEntry returns fs.Entry for the specified path, the result will be one of supported entry types: fs.File, fs.Directory, fs.Symlink
// or fs.UnsupportedEntry.
func NewEntry(path string) (fs.Entry, error) {
	path = filepath.Clean(path)

	fi, err := os.Lstat(path)
	if err != nil {
		return nil, errors.Wrap(err, "unable to determine entry type")
	}

	if path == "/" {
		return entryFromDirEntry(fi, ""), nil
	}

	return entryFromDirEntry(fi, dirPrefix(path)), nil
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

func entryFromDirEntry(fi os.FileInfo, prefix string) fs.Entry {
	isplaceholder := strings.HasSuffix(fi.Name(), ShallowEntrySuffix)
	maskedmode := fi.Mode() & os.ModeType

	switch {
	case maskedmode == os.ModeDir && !isplaceholder:
		return newFilesystemDirectory(newEntry(fi, prefix))

	case maskedmode == os.ModeDir && isplaceholder:
		return newShallowFilesystemDirectory(newEntry(fi, prefix))

	case maskedmode == os.ModeSymlink && !isplaceholder:
		return newFilesystemSymlink(newEntry(fi, prefix))

	case maskedmode == 0 && !isplaceholder:
		return newFilesystemFile(newEntry(fi, prefix))

	case maskedmode == 0 && isplaceholder:
		return newShallowFilesystemFile(newEntry(fi, prefix))

	default:
		return newFilesystemErrorEntry(newEntry(fi, prefix), fs.ErrUnknown)
	}
}

var (
	_ fs.Directory  = (*filesystemDirectory)(nil)
	_ fs.File       = (*filesystemFile)(nil)
	_ fs.Symlink    = (*filesystemSymlink)(nil)
	_ fs.ErrorEntry = (*filesystemErrorEntry)(nil)
)
