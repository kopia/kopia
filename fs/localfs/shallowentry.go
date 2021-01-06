package localfs

import (
	"context"
	"strings"

	"github.com/kopia/kopia/snapshot"
)

// Reserved suffixes to identify shallow placeholders for directories and
// files respectively.
const (
	SHALLOWDIRSUFFIX  = ".kopiadir"
	SHALLOWFILESUFFIX = ".kopiafile"
)

// TrimShallowSuffix returns the path without the placeholder suffixes.
func TrimShallowSuffix(path string) string {
	return strings.TrimSuffix(strings.TrimSuffix(path, SHALLOWDIRSUFFIX), SHALLOWFILESUFFIX)
}

// PlaceholderFilePath is a filesystem path of a shallow placeholder file.
type PlaceholderFilePath string

// DirEntryFromPlaceholder returns the snapshot.DirEntry corresponding to this PlaceholderFilePath.
func (pf PlaceholderFilePath) DirEntryFromPlaceholder(ctx context.Context) (*snapshot.DirEntry, error) {
	return dirEntryFromPlaceholder(string(pf))
}

var _ snapshot.HasDirEntryFromPlaceholder = PlaceholderFilePath("")
