//go:build freebsd || darwin || openbsd
// +build freebsd darwin openbsd

package tempfile

import (
	"os"
)

// CreateAutoDelete creates a temporary file that does not need to be explicitly removed on close.
func CreateAutoDelete() (*os.File, error) {
	return createUnixFallback()
}
