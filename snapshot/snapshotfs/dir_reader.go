package snapshotfs

import (
	"bufio"
	"io"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/jsonstream"
	"github.com/kopia/kopia/snapshot"
)

var directoryStreamType = "kopia:directory"

// readDirEntries reads all the Entry from the specified reader.
func readDirEntries(r io.Reader) ([]*snapshot.DirEntry, *fs.DirectorySummary, error) {
	var summ fs.DirectorySummary
	psr, err := jsonstream.NewReader(bufio.NewReader(r), directoryStreamType, &summ)
	if err != nil {
		return nil, nil, err
	}
	var entries []*snapshot.DirEntry
	for {
		e := &snapshot.DirEntry{}
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
