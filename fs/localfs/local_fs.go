package localfs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/logging"
)

const (
	numEntriesToRead   = 100 // number of directory entries to read in one shot
	dirListingPrefetch = 200 // number of directory items to os.Lstat() in advance
)

var log = logging.GetContextLoggerFunc("kopia/localfs")

type sortedEntries fs.Entries

func (e sortedEntries) Len() int      { return len(e) }
func (e sortedEntries) Swap(i, j int) { e[i], e[j] = e[j], e[i] }
func (e sortedEntries) Less(i, j int) bool {
	return e[i].Name() < e[j].Name()
}

type filesystemEntry struct {
	name       string
	size       int64
	mtimeNanos int64
	mode       os.FileMode
	owner      fs.OwnerInfo
	device     fs.DeviceInfo

	parentDir string
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
	return filepath.Join(e.parentDir, e.Name())
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

func newEntry(fi os.FileInfo, parentDir string) filesystemEntry {
	return filesystemEntry{
		fi.Name(),
		fi.Size(),
		fi.ModTime().UnixNano(),
		fi.Mode(),
		platformSpecificOwnerInfo(fi),
		platformSpecificDeviceInfo(fi),
		parentDir,
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

	return entryFromChildFileInfo(st, fullPath)
}

type entryWithError struct {
	entry fs.Entry
	err   error
}

func (fsd *filesystemDirectory) Readdir(ctx context.Context) (fs.Entries, error) {
	fullPath := fsd.fullPath()

	f, direrr := os.Open(fullPath) //nolint:gosec
	if direrr != nil {
		return nil, errors.Wrap(direrr, "unable to read directory")
	}
	defer f.Close() //nolint:errcheck,gosec

	// start feeding directory entry names to namesCh
	namesCh := make(chan string, dirListingPrefetch)

	var readDirErr error

	go func() {
		defer close(namesCh)

		for {
			names, err := f.Readdirnames(numEntriesToRead)
			for _, name := range names {
				namesCh <- name
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

	// launch N workers to os.Lstat() each name in parallel and push to entriesCh
	workers := 16
	for i := 0; i < workers; i++ {
		workersWG.Add(1)

		go func() {
			defer workersWG.Done()

			for n := range namesCh {
				fi, staterr := os.Lstat(fullPath + "/" + n)

				switch {
				case os.IsNotExist(staterr):
					// lost the race - ignore.
					continue
				case staterr != nil:
					entriesCh <- entryWithError{err: errors.Errorf("unable to stat directory entry %q: %v", n, staterr)}
					continue
				}

				e, fierr := entryFromChildFileInfo(fi, fullPath)
				if fierr != nil {
					log(ctx).Warningf("unable to create directory entry %q: %v", fi.Name(), fierr)
					continue
				}

				entriesCh <- entryWithError{entry: e}
			}
		}()
	}

	// close entriesCh channel when all workers terminate
	go func() {
		workersWG.Wait()
		close(entriesCh)
	}()

	// drain the entriesCh into a slice and sort it
	var entries fs.Entries

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

	sort.Sort(sortedEntries(entries))

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

	return &filesystemFile{newEntry(fi, filepath.Dir(f.Name()))}, nil
}

func (fsf *filesystemFile) Open(ctx context.Context) (fs.Reader, error) {
	f, err := os.Open(fsf.fullPath())
	if err != nil {
		return nil, errors.Wrap(err, "unable to open local file")
	}

	return &fileWithMetadata{f}, nil
}

func (fsl *filesystemSymlink) Readlink(ctx context.Context) (string, error) {
	return os.Readlink(fsl.fullPath())
}

// NewEntry returns fs.Entry for the specified path, the result will be one of supported entry types: fs.File, fs.Directory, fs.Symlink.
func NewEntry(path string) (fs.Entry, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, errors.Wrap(err, "unable to determine entry type")
	}

	switch fi.Mode() & os.ModeType {
	case os.ModeDir:
		return &filesystemDirectory{newEntry(fi, filepath.Dir(path))}, nil

	case os.ModeSymlink:
		return &filesystemSymlink{newEntry(fi, filepath.Dir(path))}, nil

	case 0:
		return &filesystemFile{newEntry(fi, filepath.Dir(path))}, nil

	default:
		return nil, errors.Errorf("unsupported filesystem entry: %v", fi)
	}
}

// Directory returns fs.Directory for the specified path.
func Directory(path string) (fs.Directory, error) {
	e, err := NewEntry(path)
	if err != nil {
		return nil, err
	}

	if d, ok := e.(fs.Directory); ok {
		return d, nil
	}

	return nil, errors.Errorf("not a directory: %v", path)
}

func entryFromChildFileInfo(fi os.FileInfo, parentDir string) (fs.Entry, error) {
	switch fi.Mode() & os.ModeType {
	case os.ModeDir:
		return &filesystemDirectory{newEntry(fi, parentDir)}, nil

	case os.ModeSymlink:
		return &filesystemSymlink{newEntry(fi, parentDir)}, nil

	case 0:
		return &filesystemFile{newEntry(fi, parentDir)}, nil

	default:
		return nil, errors.Errorf("unsupported filesystem entry: %v", fi)
	}
}

var (
	_ fs.Directory = &filesystemDirectory{}
	_ fs.File      = &filesystemFile{}
	_ fs.Symlink   = &filesystemSymlink{}
)
