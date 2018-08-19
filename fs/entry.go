package fs

import (
	"context"
	"io"
	"sort"
	"time"
)

// Entry represents a filesystem entry, which can be Directory, File, or Symlink
type Entry interface {
	Metadata() *EntryMetadata
}

// Entries is a list of entries sorted by name.
type Entries []Entry

// Reader allows reading from a file and retrieving its metadata.
type Reader interface {
	io.ReadCloser
	io.Seeker
	EntryMetadata() (*EntryMetadata, error)
}

// File represents an entry that is a file.
type File interface {
	Entry
	Open(ctx context.Context) (Reader, error)
}

// Directory represents contents of a directory.
type Directory interface {
	Entry
	Readdir(ctx context.Context) (Entries, error)
	Summary() *DirectorySummary
}

// DirectorySummary represents summary information about a directory.
type DirectorySummary struct {
	TotalFileSize    int64     `json:"size"`
	TotalFileCount   int64     `json:"files"`
	TotalDirCount    int64     `json:"dirs"`
	MaxModTime       time.Time `json:"maxTime"`
	IncompleteReason string    `json:"incomplete,omitempty"`
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
			return e[i].Metadata().Name >= n
		},
	)
	if i < len(e) && e[i].Metadata().Name == n {
		return e[i]
	}

	return nil
}

// Sort sorts the entries by name.
func (e Entries) Sort() {
	sort.Slice(e, func(i, j int) bool {
		return e[i].Metadata().Name < e[j].Metadata().Name
	})
}
