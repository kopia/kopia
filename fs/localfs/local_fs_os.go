package localfs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

type filesystemDirectoryIterator struct {
	dirHandle   *os.File
	childPrefix string

	currentIndex int
	currentBatch []os.DirEntry
}

func (it *filesystemDirectoryIterator) Next(ctx context.Context) (fs.Entry, error) {
	for {
		// we're at the end of the current batch, fetch the next batch
		if it.currentIndex >= len(it.currentBatch) {
			batch, err := it.dirHandle.ReadDir(numEntriesToRead)
			if err != nil && !errors.Is(err, io.EOF) {
				// stop iteration
				return nil, err //nolint:wrapcheck
			}

			it.currentIndex = 0
			it.currentBatch = batch

			// got empty batch
			if len(batch) == 0 {
				return nil, nil
			}
		}

		n := it.currentIndex
		it.currentIndex++

		e, err := toDirEntryOrNil(it.currentBatch[n], it.childPrefix)
		if err != nil {
			// stop iteration
			return nil, err
		}

		if e == nil {
			// go to the next item
			continue
		}

		return e, nil
	}
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

// NewEntry returns fs.Entry for the specified path, the result will be one of supported entry types: fs.File, fs.Directory, fs.Symlink
// or fs.UnsupportedEntry.
func NewEntry(path string) (fs.Entry, error) {
	path = filepath.Clean(path)

	fi, err := os.Lstat(path)
	if err != nil {
		// Paths such as `\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy01`
		// cause os.Lstat to fail with "Incorrect function" error unless they
		// end with a separator. Retry the operation with the separator added.
		var e syscall.Errno
		//nolint:goconst
		if runtime.GOOS == "windows" &&
			!strings.HasSuffix(path, string(filepath.Separator)) &&
			errors.As(err, &e) && e == 1 {
			fi, err = os.Lstat(path + string(filepath.Separator))
		}

		if err != nil {
			return nil, errors.Wrap(err, "unable to determine entry type")
		}
	}

	if path == "/" {
		return entryFromDirEntry(fi, ""), nil
	}

	return entryFromDirEntry(fi, dirPrefix(path)), nil
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
