package dir

import (
	"bufio"
	"io"

	"github.com/kopia/kopia/internal/jsonstream"
)

var directoryStreamType = "kopia:directory"

// ReadEntries reads all the Entry from the specified reader.
func ReadEntries(r io.Reader) ([]*Entry, *Summary, error) {
	var summ Summary
	psr, err := jsonstream.NewReader(bufio.NewReader(r), directoryStreamType, &summ)
	if err != nil {
		return nil, nil, err
	}
	var entries []*Entry
	for {
		e := &Entry{}
		err := psr.Read(e)
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, nil, err
		}

		entries = append(entries, e)
	}

	return entries, &summ, nil
}
