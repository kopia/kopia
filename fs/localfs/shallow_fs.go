package localfs

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/atomicfile"
	"github.com/kopia/kopia/snapshot"
)

// Helpers to implement storing of "shallow" placeholders for files or
// directory trees in a restore image. A placeholder for directory d1 is
// d1.kopia-entry/.kopia-entry. As a result, d1.kopia-entry will stat as
// a directory and show up nicely in colorized ls output. A placeholder
// for a file f1 is f1.kopia-entry.

func placeholderPath(path string, et snapshot.EntryType) (string, error) {
	switch et {
	case snapshot.EntryTypeFile:
		return path + ShallowEntrySuffix, nil
	case snapshot.EntryTypeDirectory: // Directories and regular files
		dirpath := path + ShallowEntrySuffix
		if err := os.MkdirAll(atomicfile.MaybePrefixLongFilenameOnWindows(dirpath), os.FileMode(dirMode)); err != nil {
			return "", errors.Wrap(err, "placeholderPath dir creation")
		}

		return filepath.Join(dirpath, ShallowEntrySuffix), nil
	default:
		// Shouldn't be used on links or other file types.
		return "", errors.Errorf("unsupported entry type: %v", et)
	}
}

// WriteShallowPlaceholder writes sufficient metadata into the placeholder
// file associated with path so that it can be roundtripped through
// snapshot/restore without needing to be realized in the local
// filesystem.
// TODO(rjk): Should the placeholder use the complete fs.Entry?
func WriteShallowPlaceholder(path string, de *snapshot.DirEntry) (string, error) {
	buffy := &bytes.Buffer{}
	encoder := json.NewEncoder(buffy)

	if err := encoder.Encode(de); err != nil {
		return "", errors.Wrapf(err, "json encoding DirEntry")
	}

	mp, err := placeholderPath(path, de.Type)
	if err != nil {
		return "", errors.Wrapf(err, "computing placeholder path: %q", path)
	}

	// Write the placeholder file.
	if err := atomicfile.Write(mp, buffy); err != nil {
		return "", errors.Wrapf(err, "error writing placeholder to %q", mp)
	}

	return mp, nil
}

func dirEntryFromPlaceholder(path string) (*snapshot.DirEntry, error) {
	b, err := os.ReadFile(atomicfile.MaybePrefixLongFilenameOnWindows(path))
	if err != nil {
		return nil, errors.Wrap(err, "dirEntryFromPlaceholder reading placeholder")
	}

	direntry := &snapshot.DirEntry{}
	buffy := bytes.NewBuffer(b)
	decoder := json.NewDecoder(buffy)

	if err := decoder.Decode(direntry); err != nil {
		return nil, errors.Wrap(err, "dirEntryFromPlaceholder JSON decoding")
	}

	return direntry, nil
}

type shallowFilesystemFile struct {
	filesystemEntry
}

type shallowFilesystemDirectory struct {
	filesystemEntry
}

func checkedDirEntryFromPlaceholder(path, php string) (*snapshot.DirEntry, error) {
	if _, err := os.Lstat(atomicfile.MaybePrefixLongFilenameOnWindows(path)); err == nil {
		return nil, errors.Errorf("%q, %q exist: shallowrestore tree is corrupt probably because a previous restore into a shallow tree was interrupted", path, php)
	}

	return dirEntryFromPlaceholder(php)
}

func (fsf *shallowFilesystemFile) DirEntryOrNil(ctx context.Context) (*snapshot.DirEntry, error) {
	path := fsf.fullPath()
	php := path + ShallowEntrySuffix

	return checkedDirEntryFromPlaceholder(path, php)
}

func (fsd *shallowFilesystemDirectory) DirEntryOrNil(ctx context.Context) (*snapshot.DirEntry, error) {
	path := fsd.fullPath()
	php := filepath.Join(path+ShallowEntrySuffix, ShallowEntrySuffix)

	return checkedDirEntryFromPlaceholder(path, php)
}

func (fsf *shallowFilesystemFile) Open(ctx context.Context) (fs.Reader, error) {
	// TODO(rjk): Conceivably, we could implement all of these in terms of the repository.
	return nil, errors.New("shallowFilesystemFile.Open not supported")
}

func (fsd *shallowFilesystemDirectory) SupportsMultipleIterations() bool {
	return false
}

//nolint:revive
func (fsd *shallowFilesystemDirectory) Child(ctx context.Context, name string) (fs.Entry, error) {
	return nil, errors.New("shallowFilesystemDirectory.Child not supported")
}

func (fsd *shallowFilesystemDirectory) Iterate(ctx context.Context) (fs.DirectoryIterator, error) {
	return nil, errors.New("shallowFilesystemDirectory.IterateEntries not supported")
}

var (
	_ snapshot.HasDirEntryOrNil = &shallowFilesystemFile{}
	_ snapshot.HasDirEntryOrNil = &shallowFilesystemDirectory{}
	_ fs.Directory              = &shallowFilesystemDirectory{}
	_ fs.File                   = &shallowFilesystemFile{}
)
