// Package virtualfs implements an in-memory abstraction of fs.Directory and fs.StreamingFile.
package virtualfs

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/clock"
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

func (e *virtualEntry) Close() {
}

// staticDirectory is an in-memory implementation of fs.Directory.
type staticDirectory struct {
	virtualEntry
	entries []fs.Entry
}

// Child gets the named child of a directory.
func (sd *staticDirectory) Child(ctx context.Context, name string) (fs.Entry, error) {
	//nolint:wrapcheck
	return fs.IterateEntriesAndFindChild(ctx, sd, name)
}

func (sd *staticDirectory) Iterate(ctx context.Context) (fs.DirectoryIterator, error) {
	return fs.StaticIterator(append([]fs.Entry{}, sd.entries...), nil), nil
}

func (sd *staticDirectory) SupportsMultipleIterations() bool {
	return true
}

// NewStaticDirectory returns a virtual static directory.
func NewStaticDirectory(name string, entries []fs.Entry) fs.Directory {
	return &staticDirectory{
		virtualEntry: virtualEntry{
			name: name,
			mode: defaultPermissions | os.ModeDir,
		},
		entries: entries,
	}
}

type streamingDirectory struct {
	virtualEntry

	mu sync.Mutex

	// +checklocks:mu
	iter fs.DirectoryIterator
}

var errChildNotSupported = errors.New("streamingDirectory.Child not supported")

func (sd *streamingDirectory) Child(ctx context.Context, _ string) (fs.Entry, error) {
	return nil, errChildNotSupported
}

var errIteratorAlreadyUsed = errors.New("cannot use streaming directory iterator more than once") // +checklocksignore: mu

func (sd *streamingDirectory) Iterate(ctx context.Context) (fs.DirectoryIterator, error) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	if sd.iter == nil {
		return nil, errIteratorAlreadyUsed
	}

	it := sd.iter
	sd.iter = nil

	return it, nil
}

func (sd *streamingDirectory) SupportsMultipleIterations() bool {
	return false
}

// NewStreamingDirectory returns a directory that will invoke the provided iterator
// on Iterate().
func NewStreamingDirectory(
	name string,
	iter fs.DirectoryIterator,
) fs.Directory {
	return &streamingDirectory{
		virtualEntry: virtualEntry{
			name: name,
			mode: defaultPermissions | os.ModeDir,
		},
		iter: iter,
	}
}

// virtualFile is an implementation of fs.StreamingFile with an io.Reader.
type virtualFile struct {
	virtualEntry
	reader io.ReadCloser
}

var errReaderAlreadyUsed = errors.New("cannot use streaming file reader more than once")

// GetReader returns the streaming file's reader.
// Note: Caller of this function has to ensure concurrency safety.
// The file's reader is set to nil after the first call.
func (vf *virtualFile) GetReader(ctx context.Context) (io.ReadCloser, error) {
	if vf.reader == nil {
		return nil, errReaderAlreadyUsed
	}

	// reader must be fetched only once
	ret := vf.reader
	vf.reader = nil

	return ret, nil
}

// StreamingFileFromReader returns a streaming file with given name and reader.
func StreamingFileFromReader(name string, reader io.ReadCloser) fs.StreamingFile {
	return StreamingFileWithModTimeFromReader(name, clock.Now(), reader)
}

// StreamingFileWithModTimeFromReader returns a streaming file with given name, modified time, and reader.
func StreamingFileWithModTimeFromReader(name string, t time.Time, reader io.ReadCloser) fs.StreamingFile {
	return &virtualFile{
		virtualEntry: virtualEntry{
			name:    name,
			mode:    defaultPermissions,
			modTime: t,
		},
		reader: reader,
	}
}

var (
	_ fs.Directory     = &staticDirectory{}
	_ fs.Directory     = &streamingDirectory{}
	_ fs.StreamingFile = &virtualFile{}
	_ fs.Entry         = &virtualEntry{}
)
