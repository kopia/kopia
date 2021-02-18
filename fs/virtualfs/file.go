package virtualfs

import (
	"bytes"
	"context"
	"os"

	"github.com/kopia/kopia/fs"
)

// File is an in-memory implementation of fs.File.
type File struct {
	entry

	source func() (ReaderSeekerCloser, error)
}

var _ fs.File = (*File)(nil)

// Open opens the file for reading.
func (imf *File) Open(ctx context.Context) (fs.Reader, error) {
	r, err := imf.source()
	if err != nil {
		return nil, err
	}

	return &fileReader{
		ReaderSeekerCloser: r,
		entry:              imf,
	}, nil
}

// FileWithSource returns a file with given name, permissions and source.
func FileWithSource(name string, permissions os.FileMode, source func() (ReaderSeekerCloser, error)) *File {
	return &File{
		entry: entry{
			name: name,
			mode: permissions,
			// TODO: add owner and other information
		},
		source: source,
	}
}

// FileWithContent returns a file with given content.
func FileWithContent(name string, permissions os.FileMode, content []byte) *File {
	s := func() (ReaderSeekerCloser, error) {
		return readSeekerWrapper{bytes.NewReader(content)}, nil
	}

	return FileWithSource(name, permissions, s)
}
