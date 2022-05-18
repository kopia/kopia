// Package virtualfs implements an in-memory abstraction of fs.Directory and fs.StreamingFile.
package virtualfs

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/kopia/kopia/fs"
)

const (
	defaultPermissions os.FileMode = 0o777
)

// virtualEntry is an in-memory implementation of a directory entry.
type virtualEntry struct {
	name    string
	mode    os.FileMode
	size    int64
	modTime time.Time
	owner   fs.OwnerInfo
	device  fs.DeviceInfo
}

func (e *virtualEntry) Name() string {
	return e.name
}

func (e *virtualEntry) IsDir() bool {
	return e.mode.IsDir()
}

func (e *virtualEntry) Mode() os.FileMode {
	return e.mode
}

func (e *virtualEntry) ModTime() time.Time {
	return e.modTime
}

func (e *virtualEntry) Size() int64 {
	return e.size
}

func (e *virtualEntry) Sys() interface{} {
	return nil
}

func (e *virtualEntry) Owner() fs.OwnerInfo {
	return e.owner
}

func (e *virtualEntry) Device() fs.DeviceInfo {
	return e.device
}

func (e *virtualEntry) LocalFilesystemPath() string {
	return ""
}

// staticDirectory is an in-memory implementation of fs.Directory.
type staticDirectory struct {
	virtualEntry
	entries fs.Entries
}

// Child gets the named child of a directory.
func (sd *staticDirectory) Child(ctx context.Context, name string) (fs.Entry, error) {
	// nolint:wrapcheck
	return fs.ReadDirAndFindChild(ctx, sd, name)
}

// Readdir gets the contents of a directory.
func (sd *staticDirectory) Readdir(ctx context.Context) (fs.Entries, error) {
	return append(fs.Entries(nil), sd.entries...), nil
}

func (sd *staticDirectory) IterateEntries(ctx context.Context, cb func(context.Context, fs.Entry) error) error {
	entries := append(fs.Entries(nil), sd.entries...)

	for _, e := range entries {
		if err := cb(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

// NewStaticDirectory returns a virtual static directory.
func NewStaticDirectory(name string, entries fs.Entries) fs.Directory {
	return &staticDirectory{
		virtualEntry: virtualEntry{
			name: name,
			mode: defaultPermissions | os.ModeDir,
		},
		entries: entries,
	}
}

// virtualFile is an implementation of fs.StreamingFile with an io.Reader.
type virtualFile struct {
	virtualEntry
	reader io.Reader
}

var errReaderAlreadyUsed = errors.New("cannot use streaming file reader more than once")

// GetReader returns the streaming file's reader.
// Note: Caller of this function has to ensure concurrency safety.
// The file's reader is set to nil after the first call.
func (vf *virtualFile) GetReader(ctx context.Context) (io.Reader, error) {
	if vf.reader == nil {
		return nil, errReaderAlreadyUsed
	}

	// reader must be fetched only once
	ret := vf.reader
	vf.reader = nil

	return ret, nil
}

// StreamingFileFromReader returns a streaming file with given name and reader.
func StreamingFileFromReader(name string, reader io.Reader) fs.StreamingFile {
	return &virtualFile{
		virtualEntry: virtualEntry{
			name: name,
			mode: defaultPermissions,
		},
		reader: reader,
	}
}

var (
	_ fs.Directory     = &staticDirectory{}
	_ fs.StreamingFile = &virtualFile{}
	_ fs.Entry         = &virtualEntry{}
)
