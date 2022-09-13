// Package tempfile provides a cross-platform abstraction for creating private
// read-write temporary files which are automatically deleted when closed.
package tempfile

import "os"

func tempDirOr(dir string) string {
	if dir != "" {
		return dir
	}

	return os.TempDir()
}
