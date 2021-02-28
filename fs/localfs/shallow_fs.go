package localfs

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/natefinch/atomic"
	"github.com/pkg/errors"

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
		return path + SHALLOWENTRYSUFFIX, nil
	case snapshot.EntryTypeDirectory: // Directories and regular files
		dirpath := path + SHALLOWENTRYSUFFIX
		if err := os.MkdirAll(dirpath, os.FileMode(dIRMODE)); err != nil {
			return "", errors.Wrap(err, "placeholderPath dir creation")
		}

		return filepath.Join(dirpath, SHALLOWENTRYSUFFIX), nil
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
	if err := atomic.WriteFile(mp, buffy); err != nil {
		return "", errors.Wrapf(err, "error writing placeholder to %q", mp)
	}

	return mp, nil
}

// ReadShallowPlaceholder returns the decoded ShallowMetadata for path if it exists
// regardless of the placeholder type.
func ReadShallowPlaceholder(path string) (*snapshot.DirEntry, error) {
	originalpresent := false
	if _, err := os.Lstat(path); err == nil {
		originalpresent = true
	}

	// Otherwise, the path should be a placeholder.
	php := path + SHALLOWENTRYSUFFIX

	fi, err := os.Lstat(php)

	switch {
	case err == nil && originalpresent:
		return nil, errors.Errorf("%q, %q exist: shallowrestore tree is corrupt probably because a previous restore into a shallow tree was interrupted", path, php)
	case err == nil && fi.IsDir():
		php = filepath.Join(php, SHALLOWENTRYSUFFIX)
	}

	if de, err := dirEntryFromPlaceholder(php); err == nil {
		return de, nil
	}

	if originalpresent {
		// The original path exists and there is no placeholder.
		return nil, nil
	}

	return nil, errors.Errorf("didn't find original or placeholder for %q", path)
}

func dirEntryFromPlaceholder(path string) (*snapshot.DirEntry, error) {
	b, err := ioutil.ReadFile(path) //nolint:gosec
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

var (
	// Make sure we implement HasDirEntryFromPlaceholder.
	_ snapshot.HasDirEntryOrNil = (*filesystemFile)(nil)
	_ snapshot.HasDirEntryOrNil = (*filesystemDirectory)(nil)
)
