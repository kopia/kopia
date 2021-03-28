package restore

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/kopia/kopia/fs/localfs"
)

// PathIfPlaceholder returns the placeholder suffix trimmed from path and
// true if path is a placeholder directory or file path. Otherwise,
// returns path unchanged and false.
func PathIfPlaceholder(path string) string {
	if strings.HasSuffix(path, localfs.SHALLOWENTRYSUFFIX) {
		return localfs.TrimShallowSuffix(path)
	}

	return ""
}

// SafeRemoveAll removes the shallow placeholder file(s) for path if they
// exist without experiencing errors caused by long file names.
func SafeRemoveAll(path string) error {
	if len(filepath.Base(path))+len(localfs.SHALLOWENTRYSUFFIX) <= syscall.NAME_MAX {
		return os.RemoveAll(path + localfs.SHALLOWENTRYSUFFIX)
	}

	// path can't possibly exist because we could have never written a file
	// whose path name is too long.
	return nil
}
