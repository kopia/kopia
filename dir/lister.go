package dir

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)

// Listing encapsulates list of items in a directory.
type Listing struct {
	Entries []*Entry
}

func findEntryByName(entries []*Entry, name string) *Entry {
	i := sort.Search(
		len(entries),
		func(i int) bool { return entries[i].Name >= name },
	)

	if i < len(entries) && entries[i].Name == name {
		return entries[i]
	}

	return nil
}

func (l Listing) FindEntryName(name string) *Entry {
	return findEntryByName(l.Entries, name)
}

func (l Listing) String() string {
	s := ""
	for i, f := range l.Entries {
		s += fmt.Sprintf("entry[%v] = %v\n", i, f)
	}

	return s
}

// Lister lists contents of filesystem directories.
type Lister interface {
	List(path string) (Listing, error)
}

type filesystemLister struct {
}

func (l *filesystemLister) List(path string) (Listing, error) {
	listing := Listing{}
	entries, err := ioutil.ReadDir(path)
	if err != nil {
		return listing, err
	}

	for _, fi := range entries {
		switch fi.Name() {
		case ".":
		case "..":
			continue
		}

		e, err := entryFromFileSystemInfo(path, fi)
		if err != nil {
			return listing, err
		}

		listing.Entries = append(listing.Entries, e)
	}

	return listing, nil
}

// NewFilesystemLister creates a Lister that can be used to list contents of filesystem directories.
func NewFilesystemLister() Lister {
	return &filesystemLister{}
}

func entryFromFileSystemInfo(parentDir string, fi os.FileInfo) (*Entry, error) {
	e := &Entry{
		Name:    fi.Name(),
		Mode:    int16(fi.Mode().Perm()),
		ModTime: fi.ModTime(),
		Type:    FileModeToType(fi.Mode()),
	}

	if e.Type == EntryTypeFile {
		e.Size = fi.Size()
	}

	if err := populatePlatformSpecificEntryDetails(e, fi); err != nil {
		return nil, err
	}

	return e, nil
}
