package tempfile

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

const permissions = 0o600

// CreateAutoDelete creates a temporary file that will be automatically deleted on close.
func CreateAutoDelete() (*os.File, error) {
	dir := os.TempDir()

	// on reasonably modern Linux (3.11 and above) O_TMPFILE is supported,
	// which creates invisible, unlinked file in a given directory.
	fd, err := unix.Open(dir, unix.O_RDWR|unix.O_TMPFILE|unix.O_CLOEXEC, permissions)
	if err == nil {
		return os.NewFile(uintptr(fd), ""), nil
	}

	if errors.Is(err, syscall.EISDIR) || errors.Is(err, syscall.EOPNOTSUPP) {
		return createUnixFallback()
	}

	return nil, &os.PathError{
		Op:   "open",
		Path: dir,
		Err:  err,
	}
}
