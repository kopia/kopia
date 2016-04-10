package fs

import (
	"io"
	"os"
	"sort"
)

const (
	directoryReadAhead = 1024
)

// Lister lists contents of filesystem directories.
type Lister interface {
	List(path string) (Directory, error)
}

type filesystemLister struct {
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
			e := &Entry{
				Name:     fi.Name(),
				FileMode: fi.Mode(),
				ModTime:  fi.ModTime(),
			}

			if fi.Mode().IsRegular() {
				e.FileSize = fi.Size()
			}

			e.populatePlatformSpecificEntryDetails(fi)

			dir = append(dir, e)
		}
		if err == nil {
			continue
		}
		if err == io.EOF {
			break
		}
		return nil, err
	}

	sort.Sort(sortedDirectory(dir))

	return dir, nil
}
