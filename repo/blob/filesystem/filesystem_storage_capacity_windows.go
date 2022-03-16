//go:build windows
// +build windows

package filesystem

import (
	"context"

	"github.com/pkg/errors"
	"golang.org/x/sys/windows"

	"github.com/kopia/kopia/repo/blob"
)

func (fs *fsStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	var c blob.Capacity

	pathPtr, err := windows.UTF16PtrFromString(fs.RootPath)
	if err != nil {
		return blob.Capacity{}, errors.Wrap(err, "windows GetCapacity")
	}

	err = windows.GetDiskFreeSpaceEx(pathPtr, nil, &c.SizeB, &c.FreeB)
	if err != nil {
		return blob.Capacity{}, errors.Wrap(err, "windows GetCapacity")
	}

	return c, nil
}
