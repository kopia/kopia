package fs

import (
	"context"
	"io"
	"os"
	"sort"
	"time"

	"github.com/pkg/errors"
)

// ErrUnknown is returned by ErrorEntry.ErrorInfo() to indicate that type of an entry is unknown.
var ErrUnknown = errors.Errorf("unknown or unsupported entry type")

// Entry represents a filesystem entry, which can be Directory, File, or Symlink.
type Entry interface {
	os.FileInfo
	Owner() OwnerInfo
	Device() DeviceInfo
	LocalFilesystemPath() string // returns full local filesystem path or "" if not a local filesystem
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

// Entries is a list of entries sorted by name.
type Entries []Entry

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
	GetReader(ctx context.Context) (io.Reader, error)
}

// Directory represents contents of a directory.
type Directory interface {
	Entry
	Child(ctx context.Context, name string) (Entry, error)
	IterateEntries(ctx context.Context, cb func(context.Context, Entry) error) error
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

// ErrEntryNotFound is returned when an entry is not found.
var ErrEntryNotFound = errors.New("entry not found")

// IterateEntriesAndFindChild iterates through entries from a directory and returns one by name.
// This is a convenience function that may be helpful in implementations of Directory.Child().
func IterateEntriesAndFindChild(ctx context.Context, d Directory, name string) (Entry, error) {
	var result Entry

	if err := d.IterateEntries(ctx, func(c context.Context, e Entry) error {
		if result == nil && e.Name() == name {
			result = e
		}
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "error reading directory")
	}

	if result == nil {
		return nil, ErrEntryNotFound
	}

	return result, nil
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
	TotalFileSize     int64     `json:"size"`
	TotalFileCount    int64     `json:"files"`
	TotalSymlinkCount int64     `json:"symlinks"`
	TotalDirCount     int64     `json:"dirs"`
	MaxModTime        time.Time `json:"maxTime"`
	IncompleteReason  string    `json:"incomplete,omitempty"`

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
}

// FindByName returns an entry with a given name, or nil if not found.
func (e Entries) FindByName(n string) Entry {
	i := sort.Search(
		len(e),
		func(i int) bool {
			return e[i].Name() >= n
		},
	)
	if i < len(e) && e[i].Name() == n {
		return e[i]
	}

	return nil
}

// Update returns a copy of Entries with the provided entry included, by either replacing
// existing entry with the same name or inserted in the appropriate place to maintain sorted order.
func (e Entries) Update(newEntry Entry) Entries {
	name := newEntry.Name()
	pos := sort.Search(len(e), func(i int) bool {
		return e[i].Name() >= name
	})

	// append at the end
	if pos >= len(e) {
		return append(append(Entries(nil), e...), newEntry)
	}

	if e[pos].Name() == name {
		if pos > 0 {
			return append(append(append(Entries(nil), e[0:pos]...), newEntry), e[pos+1:]...)
		}

		return append(append(Entries(nil), newEntry), e[pos+1:]...)
	}

	if pos > 0 {
		return append(append(append(Entries(nil), e[0:pos]...), newEntry), e[pos:]...)
	}

	return append(append(Entries(nil), newEntry), e[pos:]...)
}

// Remove returns a copy of Entries with the provided entry removed, while maintaining sorted order.
func (e Entries) Remove(name string) Entries {
	pos := sort.Search(len(e), func(i int) bool {
		return e[i].Name() >= name
	})

	// not found
	if pos >= len(e) {
		return e
	}

	if e[pos].Name() != name {
		return e
	}

	if pos > 0 {
		return append(append(Entries(nil), e[0:pos]...), e[pos+1:]...)
	}

	return append(Entries(nil), e[pos+1:]...)
}

// Sort sorts the entries by name.
func (e Entries) Sort() {
	sort.Slice(e, func(i, j int) bool {
		return e[i].Name() < e[j].Name()
	})
}
