package restore

import (
	"path/filepath"

	"github.com/kopia/kopia/fs/localfs"
)

// MaxFilenameLength is set for compatibility with Linux and MacOS.
const MaxFilenameLength = 255

// SafelySuffixablePath returns true if path can be suffixed with the
// placeholder suffix and written to the filesystem.
func SafelySuffixablePath(path string) bool {
	return len(filepath.Base(path))+len(localfs.ShallowEntrySuffix) <= MaxFilenameLength
}
