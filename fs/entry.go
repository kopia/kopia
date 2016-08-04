package fs

import (
	"encoding/binary"
	"hash/fnv"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/kopia/kopia/repo"
)

// Entry represents a filesystem entry, which can be Directory, File, or Symlink
type Entry interface {
	Parent() Directory
	Metadata() *EntryMetadata
}

// Entries is a list of entries sorted by name.
type Entries []Entry

// EntryMetadataReadCloser allows reading from a file and retrieving *EntryMetadata for its metadata.
type EntryMetadataReadCloser interface {
	io.ReadCloser
	EntryMetadata() (*EntryMetadata, error)
}

// File represents an entry that is a file.
type File interface {
	Entry
	Open() (EntryMetadataReadCloser, error)
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

// EntryMetadata stores attributes of a single entry in a directory.
type EntryMetadata struct {
	Name     string
	FileMode os.FileMode
	FileSize int64
	ModTime  time.Time
	OwnerID  uint32
	GroupID  uint32
	ObjectID repo.ObjectID
}

type entry struct {
	parent   Directory
	metadata *EntryMetadata
}

func (e *entry) Parent() Directory {
	return e.parent
}

func (e *entry) Metadata() *EntryMetadata {
	return e.metadata
}

func newEntry(md *EntryMetadata, parent Directory) entry {
	return entry{parent, md}
}

func (e Entries) Len() int      { return len(e) }
func (e Entries) Swap(i, j int) { e[i], e[j] = e[j], e[i] }
func (e Entries) Less(i, j int) bool {
	return e[i].Metadata().Name < e[j].Metadata().Name
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

func isLess(name1, name2 string) bool {
	if name1 == name2 {
		return false
	}

	return isLessOrEqual(name1, name2)
}

func split1(name string) (head, tail string) {
	n := strings.IndexByte(name, '/')
	if n >= 0 {
		return name[0 : n+1], name[n+1:]
	}

	return name, ""
}

func isLessOrEqual(name1, name2 string) bool {
	parts1 := strings.Split(name1, "/")
	parts2 := strings.Split(name2, "/")

	i := 0
	for i < len(parts1) && i < len(parts2) {
		if parts1[i] == parts2[i] {
			i++
			continue
		}
		if parts1[i] == "" {
			return false
		}
		if parts2[i] == "" {
			return true
		}
		return parts1[i] < parts2[i]
	}

	return len(parts1) <= len(parts2)
}

func (e *EntryMetadata) metadataHash() uint64 {
	h := fnv.New64a()
	binary.Write(h, binary.LittleEndian, e.ModTime.UnixNano())
	binary.Write(h, binary.LittleEndian, e.FileMode)
	if e.FileMode.IsRegular() {
		binary.Write(h, binary.LittleEndian, e.FileSize)
	}
	binary.Write(h, binary.LittleEndian, e.OwnerID)
	binary.Write(h, binary.LittleEndian, e.GroupID)
	return h.Sum64()
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
