package repofs

import (
	"io"

	"github.com/kopia/kopia/internal/jsonstream"
)

// directoryWriter writes a stream of directory entries.
type directoryWriter struct {
	w *jsonstream.Writer
}

// WriteEntry writes the specified entry to the output.
func (dw *directoryWriter) WriteEntry(e *dirEntry) error {
	if err := e.ObjectID.Validate(); err != nil {
		panic("invalid object ID: " + err.Error())
	}

	if e.Type == entryTypeBundle && len(e.BundledChildren) == 0 {
		panic("empty bundle!")
	}

	return dw.w.Write(e)
}

// Finalize writes the trailing data to the output stream.
func (dw *directoryWriter) Finalize() error {
	return dw.w.Finalize()
}

// newDirWriter returns new directoryWriter for with the specified output.
func newDirWriter(w io.Writer) *directoryWriter {
	dw := &directoryWriter{
		w: jsonstream.NewWriter(w, directoryStreamType),
	}

	return dw
}
