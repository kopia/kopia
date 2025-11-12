//go:build linux || freebsd || darwin || openbsd

package tempfile

import (
	"os"

	"github.com/pkg/errors"
)

// createUnixFallback creates a temporary file that does not need to be removed on close.
func createUnixFallback() (*os.File, error) {
	f, err := os.CreateTemp("", "kt-")
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	// immediately remove/unlink the file while we keep the handle open.
	if derr := os.Remove(f.Name()); derr != nil {
		f.Close() //nolint:errcheck
		return nil, errors.Wrap(derr, "unable to unlink temporary file")
	}

	return f, nil
}
