package fs

import (
	"io/ioutil"
	"os"
)

// Lister lists contents of filesystem directories.
type Lister interface {
	List(path string) (Directory, error)
}

type filesystemLister struct {
}

func (d *filesystemLister) List(path string) (Directory, error) {
	listing := Directory{}
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

func entryFromFileSystemInfo(parentDir string, fi os.FileInfo) (*Entry, error) {
	e := &Entry{
		EntryMetadata: EntryMetadata{
			Name:    fi.Name(),
			Mode:    int16(fi.Mode().Perm()),
			ModTime: fi.ModTime().UTC(),
			Type:    FileModeToType(fi.Mode()),
		},
	}

	if e.Type == EntryTypeFile {
		e.Size = fi.Size()
	}

	if err := populatePlatformSpecificEntryDetails(e, fi); err != nil {
		return nil, err
	}

	return e, nil
}
