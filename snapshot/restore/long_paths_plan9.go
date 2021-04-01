package restore

import (
	"math"

	"github.com/kopia/kopia/fs/localfs"
)

// MaxFilenameLength is the maximum length of a filename.
const MaxFilenameLength = math.MaxUint16

// SafelySuffixablePath returns true if path can be suffixed with the
// placeholder suffix and written to the filesystem.
func SafelySuffixablePath(path string) bool {
	// By code inspection, plan9 paths can't be more than MaxUint16. They
	// might need to be shorter. Each filesystem implementation may impose
	// its own limits.
	return len(path)+len(localfs.ShallowEntrySuffix) <= MaxFilenameLength
}
