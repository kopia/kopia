//go:build linux || freebsd || darwin || openbsd
// +build linux freebsd darwin openbsd

package tempfile

import (
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const permissions = 0o600

// createUnixFallback creates a temporary file that does not need to be removed on close.
func createUnixFallback(dir string) (*os.File, error) {
	fullPath := filepath.Join(tempDirOr(dir), uuid.NewString())

	f, err := os.OpenFile(fullPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, permissions) //nolint:gosec
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	// immediately remove/unlink the file while we keep the handle open.
	if derr := os.Remove(fullPath); derr != nil {
		f.Close() //nolint:errcheck
		return nil, errors.Wrap(derr, "unable to unlink temporary file")
	}

	return f, nil
}
