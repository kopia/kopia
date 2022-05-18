package localfs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

const (
	numEntriesToReadFirst    = 100 // number of directory entries to read in the first batch before parallelism kicks in.
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

type entryWithError struct {
	entry fs.Entry
	err   error
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

func (fsd *filesystemDirectory) IterateEntries(ctx context.Context, cb func(context.Context, fs.Entry) error) error {
	return fs.ReaddirToIterate(ctx, fsd, cb)
}

func (fsd *filesystemDirectory) Readdir(ctx context.Context) (fs.Entries, error) {
	fullPath := fsd.fullPath()

	f, direrr := os.Open(fullPath) //nolint:gosec
	if direrr != nil {
		return nil, errors.Wrap(direrr, "unable to read directory")
	}
	defer f.Close() //nolint:errcheck,gosec

	var entries fs.Entries

	// read first batch of directory entries using Readdir() before parallelization.
	firstBatch, firstBatchErr := f.ReadDir(numEntriesToReadFirst)
	if firstBatchErr != nil && !errors.Is(firstBatchErr, io.EOF) {
		return nil, errors.Wrap(firstBatchErr, "unable to read directory entries")
	}

	childPrefix := fullPath + string(filepath.Separator)

	for _, de := range firstBatch {
		e, err := toDirEntryOrNil(de, childPrefix)
		if err != nil {
			return nil, errors.Wrap(err, "error reading entry")
		}

		if e != nil {
			entries = append(entries, e)
		}
	}

	// first batch was complete with EOF, we're done here.
	if errors.Is(firstBatchErr, io.EOF) {
		entries.Sort()

		return entries, nil
	}

	// first batch was shorter than expected, perform another read to make sure we get EOF.
	if len(firstBatch) < numEntriesToRead {
		secondBatch, secondBatchErr := f.ReadDir(numEntriesToRead)
		if secondBatchErr != nil && !errors.Is(secondBatchErr, io.EOF) {
			return nil, errors.Wrap(secondBatchErr, "unable to read directory entries")
		}

		// process results in case it's not EOF.
		for _, de := range secondBatch {
			e, err := toDirEntryOrNil(de, childPrefix)
			if err != nil {
				return nil, errors.Wrap(err, "error reading entry")
			}

			if e != nil {
				entries = append(entries, e)
			}
		}

		// if we got EOF at this point, return.
		if errors.Is(secondBatchErr, io.EOF) {
			entries.Sort()

			return entries, nil
		}
	}

	return fsd.readRemainingDirEntriesInParallel(childPrefix, entries, f)
}

func (fsd *filesystemDirectory) readRemainingDirEntriesInParallel(childPrefix string, entries fs.Entries, f *os.File) (fs.Entries, error) {
	// start feeding directory entries to dirEntryCh
	dirEntryCh := make(chan os.DirEntry, dirListingPrefetch)

	var readDirErr error

	go func() {
		defer close(dirEntryCh)

		for {
			des, err := f.ReadDir(numEntriesToRead)
			for _, de := range des {
				dirEntryCh <- de
			}

			if err == nil {
				continue
			}

			if errors.Is(err, io.EOF) {
				break
			}

			readDirErr = err

			break
		}
	}()

	entriesCh := make(chan entryWithError, dirListingPrefetch)

	var workersWG sync.WaitGroup

	for i := 0; i < paralellelStatGoroutines; i++ {
		workersWG.Add(1)

		go func() {
			defer workersWG.Done()

			for de := range dirEntryCh {
				e, err := toDirEntryOrNil(de, childPrefix)
				if err != nil {
					entriesCh <- entryWithError{err: errors.Errorf("unable to stat directory entry %q: %v", de, err)}
					continue
				}

				if e != nil {
					entriesCh <- entryWithError{entry: e}
				}
			}
		}()
	}

	// close entriesCh channel when all goroutines terminate
	go func() {
		workersWG.Wait()
		close(entriesCh)
	}()

	// drain the entriesCh into a slice and sort it

	for e := range entriesCh {
		if e.err != nil {
			// only return the first error
			if readDirErr == nil {
				readDirErr = e.err
			}

			continue
		}

		entries = append(entries, e.entry)
	}

	entries.Sort()

	// return any error encountered when listing or reading the directory
	return entries, readDirErr
}

type fileWithMetadata struct {
	*os.File
}

func (f *fileWithMetadata) Entry() (fs.Entry, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "unable to stat() local file")
	}

	return &filesystemFile{newEntry(fi, dirPrefix(f.Name()))}, nil
}

func (fsf *filesystemFile) Open(ctx context.Context) (fs.Reader, error) {
	f, err := os.Open(fsf.fullPath())
	if err != nil {
		return nil, errors.Wrap(err, "unable to open local file")
	}

	return &fileWithMetadata{f}, nil
}

func (fsl *filesystemSymlink) Readlink(ctx context.Context) (string, error) {
	// nolint:wrapcheck
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
		return &filesystemDirectory{e.filesystemEntry}, nil

	default:
		return nil, errors.Errorf("not a directory: %v (was %T)", path, e)
	}
}

func entryFromDirEntry(fi os.FileInfo, prefix string) fs.Entry {
	isplaceholder := strings.HasSuffix(fi.Name(), ShallowEntrySuffix)
	maskedmode := fi.Mode() & os.ModeType

	switch {
	case maskedmode == os.ModeDir && !isplaceholder:
		return &filesystemDirectory{newEntry(fi, prefix)}

	case maskedmode == os.ModeDir && isplaceholder:
		return &shallowFilesystemDirectory{newEntry(fi, prefix)}

	case maskedmode == os.ModeSymlink && !isplaceholder:
		return &filesystemSymlink{newEntry(fi, prefix)}

	case maskedmode == 0 && !isplaceholder:
		return &filesystemFile{newEntry(fi, prefix)}

	case maskedmode == 0 && isplaceholder:
		return &shallowFilesystemFile{newEntry(fi, prefix)}

	default:
		return &filesystemErrorEntry{newEntry(fi, prefix), fs.ErrUnknown}
	}
}

var (
	_ fs.Directory  = &filesystemDirectory{}
	_ fs.File       = &filesystemFile{}
	_ fs.Symlink    = &filesystemSymlink{}
	_ fs.ErrorEntry = &filesystemErrorEntry{}
)
