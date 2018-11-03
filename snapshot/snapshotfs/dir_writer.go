package snapshotfs

import (
	"io"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/jsonstream"
	"github.com/kopia/kopia/snapshot"
)

// dirWriter writes a stream of directory entries.
type dirWriter struct {
	w *jsonstream.Writer
}

// WriteEntry writes the specified entry to the output.
func (dw *dirWriter) WriteEntry(e *snapshot.DirEntry) error {
	if err := e.ObjectID.Validate(); err != nil {
		panic("invalid object ID: " + err.Error())
	}

	return dw.w.Write(e)
}

// Finalize writes the trailing data to the output stream.
func (dw *dirWriter) Finalize(summ *fs.DirectorySummary) error {
	return dw.w.FinalizeWithSummary(summ)
}

// newDirWriter returns new directoryWriter for with the specified output.
func newDirWriter(w io.Writer) *dirWriter {
	dw := &dirWriter{
		w: jsonstream.NewWriter(w, directoryStreamType),
	}

	return dw
}
