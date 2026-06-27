package localfs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

const separatorStr = string(filepath.Separator)

type filesystemDirectoryIterator struct {
	dirHandle   *os.File
	childPrefix string
	opts        Options

	currentIndex int
	currentBatch []os.DirEntry
}

func (it *filesystemDirectoryIterator) Next(_ context.Context) (fs.Entry, error) {
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

		e, err := toDirEntryOrNil(it.currentBatch[n], it.childPrefix, it.opts)
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

func (fsd *filesystemDirectory) Iterate(_ context.Context) (fs.DirectoryIterator, error) {
	fullPath := fsd.fullPath()

	d, err := os.Open(fullPath + trailingSeparator(fsd)) //nolint:gosec
	if err != nil {
		return nil, errors.Wrap(err, "unable to read directory")
	}

	childPrefix := fullPath + separatorStr

	return &filesystemDirectoryIterator{dirHandle: d, childPrefix: childPrefix, opts: fsd.opts}, nil
}

func (fsd *filesystemDirectory) Child(_ context.Context, name string) (fs.Entry, error) {
	fullPath := fsd.fullPath()

	st, err := os.Lstat(filepath.Join(fullPath, name))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fs.ErrEntryNotFound
		}

		return nil, errors.Wrap(err, "unable to get child")
	}

	return entryFromDirEntry(name, st, fullPath+separatorStr, fsd.opts), nil
}

func toDirEntryOrNil(dirEntry os.DirEntry, prefix string, opts Options) (fs.Entry, error) {
	n := dirEntry.Name()

	switch fi, err := os.Lstat(prefix + n); {
	case err == nil:
		return entryFromDirEntry(n, fi, prefix, opts), nil
	case os.IsNotExist(err):
		return nil, nil
	case os.IsPermission(err):
		// For permission denied errors, return an ErrorEntry instead of failing
		// the entire directory iteration. This allows the upload process to
		// handle the error according to the configured error handling policy
		// and continue processing other entries in the directory.
		//
		// This is particularly important for inaccessible mount points such as
		// FUSE/sshfs mounts owned by another user. If an error is returned here
		// then a single inaccessible entry causes the entire containing directory
		// to fail and be omitted from the snapshot, which results in omitting
		// other accessible entries in the same directory.
		e := filesystemEntry{
			name:       TrimShallowSuffix(n),
			size:       0,
			mtimeNanos: 0,
			mode:       dirEntry.Type(),
			owner:      fs.OwnerInfo{},
			device:     fs.DeviceInfo{},
			prefix:     prefix,
			opts:       opts,
		}

		return newFilesystemErrorEntry(e, err), nil
	default:
		return nil, errors.Wrap(err, "error reading directory")
	}
}

// NewEntry returns fs.Entry for the specified path with default Options,
// the result will be one of supported entry types: fs.File, fs.Directory,
// fs.Symlink or fs.UnsupportedEntry.
func NewEntry(path string) (fs.Entry, error) {
	return NewEntryWithOptions(path, Options{})
}

// NewEntryWithOptions behaves like NewEntry but configures the returned
// entry (and any child entries produced by iterating it) with opts.
func NewEntryWithOptions(path string, opts Options) (fs.Entry, error) {
	path = filepath.Clean(path)

	fi, err := os.Lstat(path)
	if err != nil {
		// Paths such as `\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy01`
		// cause os.Lstat to fail with "Incorrect function" error unless they
		// end with a separator. Retry the operation with the separator added.
		var e syscall.Errno
		if isWindows &&
			!strings.HasSuffix(path, separatorStr) &&
			errors.As(err, &e) && e == 1 {
			fi, err = os.Lstat(path + separatorStr)
		}

		if err != nil {
			return nil, errors.Wrap(err, "unable to determine entry type")
		}
	}

	if path == "/" {
		return entryFromDirEntry("/", fi, "", opts), nil
	}

	basename, prefix := splitDirPrefix(path)

	return entryFromDirEntry(basename, fi, prefix, opts), nil
}

func entryFromDirEntry(basename string, fi os.FileInfo, prefix string, opts Options) fs.Entry {
	isplaceholder := strings.HasSuffix(basename, ShallowEntrySuffix)
	maskedmode := fi.Mode() & os.ModeType

	switch {
	case maskedmode == os.ModeDir && !isplaceholder:
		return newFilesystemDirectory(newEntry(basename, fi, prefix, opts))

	case maskedmode == os.ModeDir && isplaceholder:
		return newShallowFilesystemDirectory(newEntry(basename, fi, prefix, opts))

	case maskedmode == os.ModeSymlink && !isplaceholder:
		return newFilesystemSymlink(newEntry(basename, fi, prefix, opts))

	case maskedmode == 0 && !isplaceholder:
		return newFilesystemFile(newEntry(basename, fi, prefix, opts))

	case maskedmode == 0 && isplaceholder:
		return newShallowFilesystemFile(newEntry(basename, fi, prefix, opts))

	default:
		return newFilesystemErrorEntry(newEntry(basename, fi, prefix, opts), fs.ErrUnknown)
	}
}

var _ os.FileInfo = (*filesystemEntry)(nil)

func newEntry(basename string, fi os.FileInfo, prefix string, opts Options) filesystemEntry {
	return filesystemEntry{
		name:       TrimShallowSuffix(basename),
		size:       fi.Size(),
		mtimeNanos: fi.ModTime().UnixNano(),
		mode:       fi.Mode(),
		owner:      platformSpecificOwnerInfo(fi),
		device:     platformSpecificDeviceInfo(fi),
		prefix:     prefix,
		opts:       opts,
	}
}
