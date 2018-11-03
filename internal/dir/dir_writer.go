package dir

import (
	"io"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/jsonstream"
	"github.com/kopia/kopia/snapshot"
)

// Writer writes a stream of directory entries.
type Writer struct {
	w *jsonstream.Writer
}

// WriteEntry writes the specified entry to the output.
func (dw *Writer) WriteEntry(e *snapshot.DirEntry) error {
	if err := e.ObjectID.Validate(); err != nil {
		panic("invalid object ID: " + err.Error())
	}

	return dw.w.Write(e)
}

// Finalize writes the trailing data to the output stream.
func (dw *Writer) Finalize(summ *fs.DirectorySummary) error {
	return dw.w.FinalizeWithSummary(summ)
}

// NewWriter returns new directoryWriter for with the specified output.
func NewWriter(w io.Writer) *Writer {
	dw := &Writer{
		w: jsonstream.NewWriter(w, directoryStreamType),
	}

	return dw
}
