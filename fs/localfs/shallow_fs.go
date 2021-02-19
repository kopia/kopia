package localfs

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/natefinch/atomic"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
)

// Helpers to implement storing of "shallow" placeholders for files or
// directory trees in a restore image.

func placeholderPath(path string, f fs.Entry) (string, error) {
	switch f.Mode() & os.ModeType {
	case os.ModeDir, 0: // Directories and regular files
		return path + SHALLOWENTRYSUFFIX, nil
	default:
		// Shouldn't be used on links or other file types.
		return "", errors.Errorf("unsupported filesystem entry: %v", f)
	}
}

// WriteShallowPlaceholder writes sufficient metadata into the placeholder
// file associated with path so that it can be roundtripped through
// snapshot/restore without needing to be realized in the local
// filesystem.
// TODO(rjk): Should the placeholder use the complete fs.Entry?
func WriteShallowPlaceholder(path string, f fs.Entry) (string, error) {
	mdg, ok := f.(snapshot.HasDirEntry)
	if !ok {
		return "", errors.Errorf("fs object is not HasDirEntry?")
	}

	buffy := &bytes.Buffer{}
	encoder := json.NewEncoder(buffy)

	if err := encoder.Encode(mdg.DirEntry()); err != nil {
		return "", errors.Wrapf(err, "json encoding DirEntry")
	}

	mp, err := placeholderPath(path, f)
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
	if _, err := os.Lstat(php); err == nil && originalpresent {
		return nil, errors.Errorf("%q, %q exist: shallowrestore tree is corrupt probably because a previous restore into a shallow tree was interrupted", path, php)
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
	_ snapshot.HasDirEntryFromPlaceholder = (*filesystemFile)(nil)
	_ snapshot.HasDirEntryFromPlaceholder = (*filesystemDirectory)(nil)
)
