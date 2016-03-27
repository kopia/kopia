package dir

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)

// Listing encapsulates list of items in a directory.
type Listing struct {
	Directories []*Entry
	Files       []*Entry
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

func (l Listing) FindDirectoryByName(name string) *Entry {
	return findEntryByName(l.Directories, name)
}

func (l Listing) FindFileByName(name string) *Entry {
	return findEntryByName(l.Files, name)
}

func (l Listing) String() string {
	s := ""
	for i, d := range l.Directories {
		s += fmt.Sprintf("dir[%v] = %v\n", i, d)
	}
	for i, f := range l.Files {
		s += fmt.Sprintf("file[%v] = %v\n", i, f)
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

		if e.Type == EntryTypeDirectory {
			listing.Directories = append(listing.Directories, e)
		} else {
			listing.Files = append(listing.Files, e)
		}
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
