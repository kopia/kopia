package tempfile

import (
	"errors"
	"os"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

var unsupportedTmpFile = new(int32) //nolint:gochecknoglobals

// Create creates a temporary file that will be automatically deleted on close.
func Create(dir string) (*os.File, error) {
	if atomic.LoadInt32(unsupportedTmpFile) == 1 {
		// already tried O_TMPFILE, was unsupported, fall back to generic
		// Unix method.
		return createUnixFallback(dir)
	}

	// on reasonably modern Linux (3.11 and above) O_TMPFILE is supported,
	// which creates invisible, unlinked file in a given directory.

	fd, err := unix.Open(dir, unix.O_RDWR|unix.O_TMPFILE|unix.O_CLOEXEC, permissions)
	if err == nil {
		return os.NewFile(uintptr(fd), ""), nil
	}

	if errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.EOPNOTSUPP) {
		// O_TMPFILE is unsupported, fall back and prevent future attempts.
		atomic.StoreInt32(unsupportedTmpFile, 1)

		return createUnixFallback(dir)
	}

	return nil, &os.PathError{
		Op:   "open",
		Path: dir,
		Err:  err,
	}
}
