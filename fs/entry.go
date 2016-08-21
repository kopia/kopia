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

// EntryMetadata stores attributes of a single entry in a directory.
type EntryMetadata struct {
	Name            string           `json:"name,omitempty"`
	Mode            os.FileMode      `json:"mode,omitempty"`
	FileSize        int64            `json:"size,omitempty"`
	ModTimeNano     int64            `json:"mtime,omitempty"`
	ObjectID        *repo.ObjectID   `json:"oid,omitempty"`
	UserID          uint32           `json:"uid,omitempty"`
	GroupID         uint32           `json:"gid,omitempty"`
	BundledChildren []*EntryMetadata `json:"children,omitempty"`
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

// ModTime returns the modification time.
func (e *EntryMetadata) ModTime() time.Time {
	return time.Unix(e.ModTimeNano/1000000000, e.ModTimeNano%1000000000)
}

func (e *EntryMetadata) metadataHash() uint64 {
	h := fnv.New64a()
	binary.Write(h, binary.LittleEndian, e.ModTimeNano)
	binary.Write(h, binary.LittleEndian, e.Mode)
	binary.Write(h, binary.LittleEndian, e.FileSize)
	binary.Write(h, binary.LittleEndian, e.UserID)
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
