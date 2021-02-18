package virtualfs

import (
	"context"
	"io"

	"github.com/kopia/kopia/fs"
)

// ReaderSeekerCloser implements io.Reader, io.Seeker and io.Closer.
type ReaderSeekerCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// readSeekerWrapper adds a no-op Close method to a ReadSeeker.
type readSeekerWrapper struct {
	io.ReadSeeker
}

func (rs readSeekerWrapper) Close() error {
	return nil
}

// readCloserWrapper adds a no-op Seek method to a ReadCloser.
type readCloserWrapper struct {
	io.ReadCloser
}

func (rc readCloserWrapper) Seek(start int64, offset int) (int64, error) {
	log(context.TODO()).Debugf("seek not supported: start %d, offset %d", start, offset)
	return 0, nil
}

// fileReader is an in-memory implementation of fs.Reader.
type fileReader struct {
	ReaderSeekerCloser
	entry fs.Entry
}

func (fr *fileReader) Entry() (fs.Entry, error) {
	return fr.entry, nil
}
