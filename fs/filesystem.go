package fs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const (
	directoryReadAhead = 1024
)

// EntryMetadataReadCloser allows reading from a file and retrieving *EntryMetadata for its metadata.
type EntryMetadataReadCloser interface {
	io.ReadCloser
	EntryMetadata() (*EntryMetadata, error)
}

type filesystemEntry struct {
	entry
	path string
}

type filesystemDirectory filesystemEntry
type filesystemSymlink filesystemEntry
type filesystemFile filesystemEntry

func (fsd *filesystemDirectory) Readdir() (Entries, error) {
	f, err := os.Open(fsd.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries Entries

	for {
		fileInfos, err := f.Readdir(16)
		for _, fi := range fileInfos {
			e, err := entryFromFileInfo(fi, filepath.Join(fsd.path, fi.Name()), fsd)
			if err != nil {
				return nil, err
			}
			entries = append(entries, e)
		}
		if err == nil {
			continue
		}
		if err == io.EOF {
			break
		}
		return nil, err
	}

	sort.Sort(entries)

	return entries, nil
}

type fileWithMetadata struct {
	*os.File
}

func (erc *fileWithMetadata) EntryMetadata() (*EntryMetadata, error) {
	fi, err := erc.Stat()
	if err != nil {
		return nil, err
	}

	return entryMetadataFromFileInfo(fi), nil
}

func (fsf *filesystemFile) Open() (EntryMetadataReadCloser, error) {
	f, err := os.Open(fsf.path)
	if err != nil {
		return nil, err
	}

	return &fileWithMetadata{f}, nil
}

func (fsl *filesystemSymlink) Readlink() (string, error) {
	return os.Readlink(fsl.path)
}

// NewFilesystemEntry returns fs.Entry for the specified path, the result will be one of supported entry types: File, Directory, Symlink.
func NewFilesystemEntry(path string, parent Directory) (Entry, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}

	return entryFromFileInfo(fi, path, parent)
}

// NewFilesystemDirectory returns fs.Directory for the specified path.
func NewFilesystemDirectory(path string, parent Directory) (Directory, error) {
	e, err := NewFilesystemEntry(path, parent)
	if err != nil {
		return nil, err
	}

	switch e := e.(type) {
	case Directory:
		return e, nil

	default:
		return nil, fmt.Errorf("not a directory: %v", path)
	}
}

func entryMetadataFromFileInfo(fi os.FileInfo) *EntryMetadata {
	e := &EntryMetadata{
		Name:     filepath.Base(fi.Name()),
		FileMode: fi.Mode(),
		ModTime:  fi.ModTime(),
	}

	if fi.Mode().IsRegular() {
		e.FileSize = fi.Size()
	}

	e.populatePlatformSpecificEntryDetails(fi)
	return e
}

func entryFromFileInfo(fi os.FileInfo, path string, parent Directory) (Entry, error) {
	entry := newEntry(entryMetadataFromFileInfo(fi), parent)

	switch fi.Mode() & os.ModeType {
	case os.ModeDir:
		return &filesystemDirectory{entry, path}, nil

	case os.ModeSymlink:
		return &filesystemSymlink{entry, path}, nil

	case 0:
		return &filesystemFile{entry, path}, nil

	default:
		return nil, fmt.Errorf("unsupported filesystem entry: %v", path)
	}
}

var _ Directory = &filesystemDirectory{}
var _ File = &filesystemFile{}
var _ Symlink = &filesystemSymlink{}
