package hashcache

import (
	"fmt"
	"io"

	"github.com/kopia/kopia/internal/jsonstream"
)

// Writer emits hash cache entries.
type Writer struct {
	writer          *jsonstream.Writer
	lastNameWritten string
}

// NewWriter creates new hash cache Writer.
func NewWriter(w io.Writer) *Writer {
	hcw := &Writer{
		writer: jsonstream.NewWriter(w, hashCacheStreamType),
	}
	return hcw
}

// WriteEntry emits the specified hash cache entry.
func (hcw *Writer) WriteEntry(e Entry) error {
	if err := e.ObjectID.Validate(); err != nil {
		panic("invalid object ID: " + err.Error())
	}

	if hcw.lastNameWritten != "" {
		if isLessOrEqual(e.Name, hcw.lastNameWritten) {
			return fmt.Errorf("out-of-order directory entry, previous '%v' current '%v'", hcw.lastNameWritten, e.Name)
		}
		hcw.lastNameWritten = e.Name
	}

	hcw.writer.Write(&e)

	return nil
}

// Finalize closes hashcache stream and must be invoked after emitting all entries.
func (hcw *Writer) Finalize() error {
	return hcw.writer.Finalize()
}
