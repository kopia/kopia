package localfs

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/kopia/kopia/snapshot"
)

const (
	// ShallowEntrySuffix is a suffix identifying placeholder files.
	ShallowEntrySuffix = ".kopia-entry"

	// dirMode is the FileMode for placeholder directories.
	dirMode = 0o700
)

// TrimShallowSuffix returns the path without the placeholder suffix.
func TrimShallowSuffix(path string) string {
	return strings.TrimSuffix(path, ShallowEntrySuffix)
}

// PlaceholderFilePath is a filesystem path of a shallow placeholder file or directory.
type PlaceholderFilePath string

// DirEntryOrNil returns the snapshot.DirEntry corresponding to this PlaceholderFilePath.
func (pf PlaceholderFilePath) DirEntryOrNil(ctx context.Context) (*snapshot.DirEntry, error) {
	path := string(pf)
	if fi, err := os.Lstat(path); err == nil && fi.IsDir() {
		return dirEntryFromPlaceholder(filepath.Join(path, ShallowEntrySuffix))
	}

	return dirEntryFromPlaceholder(path)
}

var _ snapshot.HasDirEntryOrNil = PlaceholderFilePath("")
