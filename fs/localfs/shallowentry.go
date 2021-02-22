package localfs

import (
	"context"
	"strings"

	"github.com/kopia/kopia/snapshot"
)

// SHALLOWENTRYSUFFIX is a suffix identifying placeholder files.
const SHALLOWENTRYSUFFIX = ".kopia-entry"

// TrimShallowSuffix returns the path without the placeholder suffix.
func TrimShallowSuffix(path string) string {
	return strings.TrimSuffix(path, SHALLOWENTRYSUFFIX)
}

// PlaceholderFilePath is a filesystem path of a shallow placeholder file.
type PlaceholderFilePath string

// DirEntryOrNil returns the snapshot.DirEntry corresponding to this PlaceholderFilePath.
func (pf PlaceholderFilePath) DirEntryOrNil(ctx context.Context) (*snapshot.DirEntry, error) {
	return dirEntryFromPlaceholder(string(pf))
}

var _ snapshot.HasDirEntryOrNil = PlaceholderFilePath("")
