package fs

import "io"

type Reader interface {
	GetEntry(path string) (Entry, error)
	ReadDirectory(path string) (Directory, error)
	Open(path string) (io.ReadCloser, error)
}
