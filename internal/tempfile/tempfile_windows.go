package tempfile

import (
	"os"
	"path/filepath"
	"syscall"

	"github.com/google/uuid"
	"golang.org/x/sys/windows"
)

// Create creates a temporary file that will be automatically deleted on close.
func Create(dir string) (*os.File, error) {
	fullpath := filepath.Join(tempDirOr(dir), uuid.NewString())

	fname, err := syscall.UTF16PtrFromString(fullpath)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	// This call creates a file that's automatically deleted on close.
	h, err := syscall.CreateFile(
		fname,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		0,
		nil,
		syscall.OPEN_ALWAYS,
		uint32(windows.FILE_FLAG_DELETE_ON_CLOSE),
		0)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	return os.NewFile(uintptr(h), fullpath), nil
}
