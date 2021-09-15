//go:build !windows && !plan9
// +build !windows,!plan9

package restore

import (
	"path/filepath"
	"syscall"

	"github.com/kopia/kopia/fs/localfs"
)

// MaxFilenameLength is the maximum length of a filename.
const MaxFilenameLength = syscall.NAME_MAX

// SafelySuffixablePath returns true if path can be suffixed with the
// placeholder suffix and written to the filesystem.
func SafelySuffixablePath(path string) bool {
	return len(filepath.Base(path))+len(localfs.ShallowEntrySuffix) <= MaxFilenameLength
}
