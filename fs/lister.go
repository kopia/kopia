package fs

import (
	"io"
	"os"
	"path/filepath"
	"sort"
)

const (
	directoryReadAhead = 1024
)

// Lister lists contents of filesystem directories.
type Lister interface {
	List(path string) (Directory, error)
	Open(path string) (io.ReadCloser, *Entry, error)
}

type filesystemLister struct {
}

func (d *filesystemLister) Open(path string) (io.ReadCloser, *Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}

	return f, entryFromFileInfo(fi), nil
}

func (d *filesystemLister) List(path string) (Directory, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var dir Directory

	for {
		fileInfos, err := f.Readdir(16)
		for _, fi := range fileInfos {
			dir = append(dir, entryFromFileInfo(fi))
		}
		if err == nil {
			continue
		}
		if err == io.EOF {
			break
		}
		return nil, err
	}

	sort.Sort(dir)

	return dir, nil
}

func entryFromFileInfo(fi os.FileInfo) *Entry {
	e := &Entry{
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
