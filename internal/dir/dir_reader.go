package dir

import (
	"bufio"
	"io"

	"github.com/kopia/kopia/internal/jsonstream"
)

var directoryStreamType = "kopia:directory"

// ReadEntries reads all the Entry from the specified reader.
func ReadEntries(r io.Reader) ([]*Entry, error) {
	psr, err := jsonstream.NewReader(bufio.NewReader(r), directoryStreamType)
	if err != nil {
		return nil, err
	}
	var entries []*Entry
	for {
		e := &Entry{}
		err := psr.Read(e)
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		entries = append(entries, e)
	}

	return entries, nil
}
