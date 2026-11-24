//go:build windows

package cache

import (
	"github.com/pkg/errors"
	"golang.org/x/sys/windows"
)

// getDiskFreeBytes returns the number of bytes available to the calling user
// on the filesystem that contains dir (respects per-user disk quotas).
func getDiskFreeBytes(dir string) (uint64, error) {
	pathPtr, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return 0, errors.Wrap(err, "windows getDiskFreeBytes")
	}

	var avail uint64

	if err = windows.GetDiskFreeSpaceEx(pathPtr, &avail, nil, nil); err != nil {
		return 0, errors.Wrap(err, "windows getDiskFreeSpaceEx")
	}

	return avail, nil
}
