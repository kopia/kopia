package snapshotfs

import (
	"encoding/json"
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
)

const directoryStreamType = "kopia:directory"

// readDirEntries reads all directory entries from the specified reader.
func readDirEntries(r io.Reader) ([]*snapshot.DirEntry, *fs.DirectorySummary, error) {
	var dir snapshot.DirManifest

	if err := json.NewDecoder(r).Decode(&dir); err != nil {
		return nil, nil, errors.Wrap(err, "unable to parse directory object")
	}

	if dir.StreamType != directoryStreamType {
		return nil, nil, errors.New("invalid directory stream type")
	}

	return dir.Entries, dir.Summary, nil
}
