package localfs

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/kopia/kopia/snapshot"
)

const (
	// SHALLOWENTRYSUFFIX is a suffix identifying placeholder files.
	SHALLOWENTRYSUFFIX = ".kopia-entry"

	// FileMode for placeholder directories.
	dIRMODE = 0700
)

// TrimShallowSuffix returns the path without the placeholder suffix.
func TrimShallowSuffix(path string) string {
	return strings.TrimSuffix(path, SHALLOWENTRYSUFFIX)
}

// PlaceholderFilePath is a filesystem path of a shallow placeholder file or directory.
type PlaceholderFilePath string

// DirEntryOrNil returns the snapshot.DirEntry corresponding to this PlaceholderFilePath.
func (pf PlaceholderFilePath) DirEntryOrNil(ctx context.Context) (*snapshot.DirEntry, error) {
	path := string(pf)
	if fi, err := os.Lstat(path); err == nil && fi.IsDir() {
		return dirEntryFromPlaceholder(filepath.Join(path, SHALLOWENTRYSUFFIX))
	}

	return dirEntryFromPlaceholder(path)
}

var _ snapshot.HasDirEntryOrNil = PlaceholderFilePath("")
