package restore

import (
	"syscall"
)

// MaxFilenameLength is the maximum length of a filename.
const MaxFilenameLength = syscall.MAX_PATH

// SafelySuffixablePath returns true if path can be suffixed with the
// placeholder suffix and written to the filesystem.
func SafelySuffixablePath(path string) bool {
	return true
}
