package fs

import (
	"context"
	"io"
	"os"
	"sort"

	"github.com/pkg/errors"
)

// ModBits is a bitmask representing the mode flags supported.
const ModBits = os.ModePerm | os.ModeSetgid | os.ModeSetuid | os.ModeSticky

// ErrUnknown is returned by ErrorEntry.ErrorInfo() to indicate that type of an entry is unknown.
var ErrUnknown = errors.New("unknown or unsupported entry type")

// Entry represents a filesystem entry, which can be Directory, File, or Symlink.
type Entry interface {
	os.FileInfo
	Owner() OwnerInfo
	Device() DeviceInfo
	LocalFilesystemPath() string // returns full local filesystem path or "" if not a local filesystem
	Close()                      // closes or recycles any resources associated with the entry, must be idempotent
}

// OwnerInfo describes owner of a filesystem entry.
type OwnerInfo struct {
	UserID  uint32 `json:"uid"`
	GroupID uint32 `json:"gid"`
}

// DeviceInfo describes the device this filesystem entry is on.
type DeviceInfo struct {
	Dev  uint64 `json:"dev"`
	Rdev uint64 `json:"rdev"`
}

// Reader allows reading from a file and retrieving its up-to-date file info.
type Reader interface {
	io.ReadCloser
	io.Seeker

	Entry() (Entry, error)
}

// File represents an entry that is a file.
type File interface {
	Entry
	Open(ctx context.Context) (Reader, error)
}

// StreamingFile represents an entry that is a stream.
type StreamingFile interface {
	Entry
	GetReader(ctx context.Context) (io.ReadCloser, error)
}

// Directory represents contents of a directory.
type Directory interface {
	Entry

	Child(ctx context.Context, name string) (Entry, error)
	Iterate(ctx context.Context) (DirectoryIterator, error)
	// SupportsMultipleIterations returns true if the Directory supports iterating
	// through the entries multiple times. Otherwise it returns false.
	SupportsMultipleIterations() bool
}

// IterateEntries iterates entries the provided directory and invokes given callback for each entry
// or until the callback returns an error.
func IterateEntries(ctx context.Context, dir Directory, cb func(context.Context, Entry) error) error {
	iter, err := dir.Iterate(ctx)
	if err != nil {
		return errors.Wrapf(err, "in fs.IterateEntries, creating iterator for directory %s", dir.Name())
	}

	defer iter.Close()

	cur, err := iter.Next(ctx)
	if err != nil {
		err = errors.Wrapf(err, "in fs.IterateEntries, on first iteration")
	}

	for cur != nil {
		if err2 := cb(ctx, cur); err2 != nil {
			return errors.Wrapf(err2, "in fs.IterateEntries, while calling callback on file %s", cur.Name())
		}

		cur, err = iter.Next(ctx)
	}

	return err //nolint:wrapcheck
}

// DirectoryIterator iterates entries in a directory.
//
// The client is expected to call Next() in a loop until it returns a nil entry to signal
// end of iteration or until an error has occurred.
//
// Valid results:
//
// (nil,nil) - end of iteration, success
// (entry,nil) - iteration in progress, success
// (nil,err) - iteration stopped, failure
//
// The behavior of calling Next() after iteration has signaled its end is undefined.
//
// To release any resources associated with iteration the client must call Close().
type DirectoryIterator interface {
	Next(ctx context.Context) (Entry, error)
	Close()
}

// DirectoryWithSummary is optionally implemented by Directory that provide summary.
type DirectoryWithSummary interface {
	Summary(ctx context.Context) (*DirectorySummary, error)
}

// ErrorEntry represents entry in a Directory that had encountered an error or is unknown/unsupported (ErrUnknown).
type ErrorEntry interface {
	Entry

	ErrorInfo() error
}

// GetAllEntries uses Iterate to return all entries in a Directory.
func GetAllEntries(ctx context.Context, d Directory) ([]Entry, error) {
	entries := []Entry{}

	iter, err := d.Iterate(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	defer iter.Close()

	cur, err := iter.Next(ctx)
	for cur != nil {
		entries = append(entries, cur)
		cur, err = iter.Next(ctx)
	}

	return entries, err //nolint:wrapcheck
}

// ErrEntryNotFound is returned when an entry is not found.
var ErrEntryNotFound = errors.New("entry not found")

// IterateEntriesAndFindChild iterates through entries from a directory and returns one by name.
// This is a convenience function that may be helpful in implementations of Directory.Child().
func IterateEntriesAndFindChild(ctx context.Context, d Directory, name string) (Entry, error) {
	iter, err := d.Iterate(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	defer iter.Close()

	cur, err := iter.Next(ctx)
	for cur != nil {
		if cur.Name() == name {
			return cur, nil
		}

		cur, err = iter.Next(ctx)
	}

	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	return nil, ErrEntryNotFound
}

// MaxFailedEntriesPerDirectorySummary is the maximum number of failed entries per directory summary.
const MaxFailedEntriesPerDirectorySummary = 10

// EntryWithError describes error encountered when processing an entry.
type EntryWithError struct {
	EntryPath string `json:"path"`
	Error     string `json:"error"`
}

// DirectorySummary represents summary information about a directory.
type DirectorySummary struct {
	TotalFileSize     int64        `json:"size"`
	TotalFileCount    int64        `json:"files"`
	TotalSymlinkCount int64        `json:"symlinks"`
	TotalDirCount     int64        `json:"dirs"`
	MaxModTime        UTCTimestamp `json:"maxTime"`
	IncompleteReason  string       `json:"incomplete,omitempty"`

	// number of failed files
	FatalErrorCount   int `json:"numFailed"`
	IgnoredErrorCount int `json:"numIgnoredErrors,omitempty"`

	// first 10 failed entries
	FailedEntries []*EntryWithError `json:"errors,omitempty"`
}

// Clone clones given directory summary.
func (s *DirectorySummary) Clone() DirectorySummary {
	res := *s

	res.FailedEntries = append([]*EntryWithError(nil), s.FailedEntries...)

	return res
}

// Symlink represents a symbolic link entry.
type Symlink interface {
	Entry
	Readlink(ctx context.Context) (string, error)
	Resolve(ctx context.Context) (Entry, error)
}

// FindByName returns an entry with a given name, or nil if not found. Assumes
// the given slice of fs.Entry is sorted.
func FindByName(entries []Entry, n string) Entry {
	i := sort.Search(
		len(entries),
		func(i int) bool {
			return entries[i].Name() >= n
		},
	)
	if i < len(entries) && entries[i].Name() == n {
		return entries[i]
	}

	return nil
}

// Sort sorts the entries by name.
func Sort(entries []Entry) {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
}
