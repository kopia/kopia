package restore

import (
	"path/filepath"
	"syscall"

	"github.com/kopia/kopia/fs/localfs"
)

// MaxFilenameLength is the maximum length of a filename.
// Go source suggests reducing this by 1.
const MaxFilenameLength = syscall.MAX_PATH - 1

// SafelySuffixablePath returns true if path can be suffixed with the
// placeholder suffix and written to the filesystem.
func SafelySuffixablePath(path string) bool {
	return len(filepath.Base(path))+len(localfs.ShallowEntrySuffix) <= MaxFilenameLength
}
