package fs

import (
	"io"
	"sort"
	"strings"
)

// Entry represents a filesystem entry, which can be Directory, File, or Symlink
type Entry interface {
	Parent() Directory
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
	Open() (Reader, error)
}

// Directory represents contents of a directory.
type Directory interface {
	Entry
	Readdir() (Entries, error)
}

// Symlink represents a symbolic link entry.
type Symlink interface {
	Entry
	Readlink() (string, error)
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

// EntryPath returns a path of a given entry from its root node.
func EntryPath(e Entry) string {
	var parts []string

	p := e
	for p != nil {
		parts = append(parts, p.Metadata().Name)
		p = p.Parent()
	}

	// Invert parts
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}

	return strings.Join(parts, "/")
}
