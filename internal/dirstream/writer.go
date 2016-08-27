package dirstream

import (
	"io"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/jsonstream"
)

// DirectoryWriter writes a stream of directory entries.
type DirectoryWriter struct {
	w *jsonstream.Writer
}

// WriteEntry writes the specified entry to the output.
func (dw *DirectoryWriter) WriteEntry(e *fs.EntryMetadata) error {
	if err := e.ObjectID.Validate(); err != nil {
		panic("invalid object ID: " + err.Error())
	}

	return dw.w.Write(e)
}

// Finalize writes the trailing data to the output stream.
func (dw *DirectoryWriter) Finalize() error {
	return dw.w.Finalize()
}

// NewWriter returns new DirectoryWriter for with the specified output.
func NewWriter(w io.Writer) *DirectoryWriter {
	dw := &DirectoryWriter{
		w: jsonstream.NewWriter(w, directoryStreamType),
	}

	return dw
}
