package fs

import (
	"io"
	"os"
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

type localStreamingDirectory struct {
	dir     *os.File
	pending []os.FileInfo
}

func (d *filesystemLister) List(path string) (Directory, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	ch := make(Directory, 16)
	go func() {
		for {
			fileInfos, err := f.Readdir(16)
			for _, fi := range fileInfos {
				ch <- entryFromFileSystemInfo(path, fi)
			}
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			ch <- EntryOrError{Error: err}
		}
		f.Close()
		close(ch)
	}()

	return ch, nil
}

func entryFromFileSystemInfo(parentDir string, fi os.FileInfo) EntryOrError {
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
		return EntryOrError{Error: err}
	}

	return EntryOrError{Entry: e}
}
