//go:build freebsd || darwin || openbsd
// +build freebsd darwin openbsd

package tempfile

import (
	"os"
)

// Create creates a temporary file that does not need to be removed on close.
func Create(dir string) (*os.File, error) {
	return createUnixFallback(dir)
}
