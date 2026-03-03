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
	options     *Options

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

		e, err := toDirEntryOrNil(it.currentBatch[n], it.childPrefix, it.options)
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

	return &filesystemDirectoryIterator{dirHandle: d, childPrefix: childPrefix, options: fsd.options}, nil
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

	return entryFromDirEntry(name, st, fullPath+separatorStr, fsd.options), nil
}

func toDirEntryOrNil(dirEntry os.DirEntry, prefix string, options *Options) (fs.Entry, error) {
	n := dirEntry.Name()

	fi, err := os.Lstat(prefix + n)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		// For permission denied errors, return an ErrorEntry instead of
		// failing the entire directory iteration. This allows the upload
		// process to handle the error according to the configured error
		// handling policy and continue processing other entries in the
		// directory.
		//
		// This is particularly important for inaccessible mount points
		// (e.g., FUSE/sshfs mounts owned by another user); without this,
		// a single inaccessible entry causes the entire containing
		// directory to fail and be omitted from the snapshot, resulting
		// in data loss.
		if os.IsPermission(err) {
			return newFilesystemErrorEntry(newEntryFromDirEntry(n, dirEntry.Type(), prefix, options), err), nil
		}

		if options != nil && options.IgnoreUnreadableDirEntries {
			return nil, nil
		}

		return nil, errors.Wrap(err, "error reading directory")
	}

	return entryFromDirEntry(n, fi, prefix, options), nil
}

// newEntryFromDirEntry creates a minimal filesystemEntry from an os.DirEntry
// when we cannot stat the file (e.g., permission denied). This uses the type
// information from the directory entry itself, with zero/default values for
// size, mtime, owner, and device since we cannot obtain them without stat.
func newEntryFromDirEntry(name string, mode os.FileMode, prefix string, options *Options) filesystemEntry {
	return filesystemEntry{
		name:       TrimShallowSuffix(name),
		size:       0,
		mtimeNanos: 0,
		mode:       mode,
		owner:      fs.OwnerInfo{},
		device:     fs.DeviceInfo{},
		prefix:     prefix,
		options:    options,
	}
}

// NewEntry returns fs.Entry for the specified path, the result will be one of supported entry types: fs.File, fs.Directory, fs.Symlink
// or fs.UnsupportedEntry.
// It uses DefaultOptions for configuration.
func NewEntry(path string) (fs.Entry, error) {
	return NewEntryWithOptions(path, DefaultOptions)
}

// NewEntryWithOptions returns fs.Entry for the specified path, the result will be one of supported entry types: fs.File, fs.Directory, fs.Symlink
// or fs.UnsupportedEntry.
// It uses the provided Options for configuration.
func NewEntryWithOptions(path string, options *Options) (fs.Entry, error) {
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
		return entryFromDirEntry("/", fi, "", options), nil
	}

	basename, prefix := splitDirPrefix(path)

	return entryFromDirEntry(basename, fi, prefix, options), nil
}

func entryFromDirEntry(basename string, fi os.FileInfo, prefix string, options *Options) fs.Entry {
	isplaceholder := strings.HasSuffix(basename, ShallowEntrySuffix)
	maskedmode := fi.Mode() & os.ModeType

	switch {
	case maskedmode == os.ModeDir && !isplaceholder:
		return newFilesystemDirectory(newEntry(basename, fi, prefix, options))

	case maskedmode == os.ModeDir && isplaceholder:
		return newShallowFilesystemDirectory(newEntry(basename, fi, prefix, options))

	case maskedmode == os.ModeSymlink && !isplaceholder:
		return newFilesystemSymlink(newEntry(basename, fi, prefix, options))

	case maskedmode == 0 && !isplaceholder:
		return newFilesystemFile(newEntry(basename, fi, prefix, options))

	case maskedmode == 0 && isplaceholder:
		return newShallowFilesystemFile(newEntry(basename, fi, prefix, options))

	default:
		return newFilesystemErrorEntry(newEntry(basename, fi, prefix, options), fs.ErrUnknown)
	}
}

var _ os.FileInfo = (*filesystemEntry)(nil)

func newEntry(basename string, fi os.FileInfo, prefix string, options *Options) filesystemEntry {
	return filesystemEntry{
		TrimShallowSuffix(basename),
		fi.Size(),
		fi.ModTime().UnixNano(),
		fi.Mode(),
		platformSpecificOwnerInfo(fi),
		platformSpecificDeviceInfo(fi),
		prefix,
		options,
	}
}
