package restore

import (
	"os"
	"strings"

	"github.com/kopia/kopia/fs/localfs"
)

// PathIfPlaceholder returns the placeholder suffix trimmed from path and
// true if path is a placeholder directory or file path. Otherwise,
// returns path unchanged and false.
func PathIfPlaceholder(path string) string {
	if strings.HasSuffix(path, localfs.ShallowEntrySuffix) {
		return localfs.TrimShallowSuffix(path)
	}

	return ""
}

// SafeRemoveAll removes the shallow placeholder file(s) for path if they
// exist without experiencing errors caused by long file names.
func SafeRemoveAll(path string) error {
	if SafelySuffixablePath(path) {
		return os.RemoveAll(path + localfs.ShallowEntrySuffix)
	}

	// path can't possibly exist because we could have never written a file
	// whose path name is too long.
	return nil
}
