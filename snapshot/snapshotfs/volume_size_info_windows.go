//go:build windows

package snapshotfs

import (
	"math"

	"golang.org/x/sys/windows"

	"github.com/kopia/kopia/repo/blob"
)

func getPlatformVolumeSizeInfo(volumeMountPoint string) (volumeSizeInfo, error) {
	var c blob.Capacity

	pathPtr, err := windows.UTF16PtrFromString(volumeMountPoint)
	if err != nil {
		return volumeSizeInfo{}, err //nolint:wrapcheck
	}

	err = windows.GetDiskFreeSpaceEx(pathPtr, nil, &c.SizeB, &c.FreeB)
	if err != nil {
		return volumeSizeInfo{}, err //nolint:wrapcheck
	}

	return volumeSizeInfo{
		totalSize:  c.SizeB,
		usedSize:   c.SizeB - c.FreeB,
		filesCount: int64(math.MaxInt64), // On Windows it's not possible to get / estimate number of files on volume
	}, nil
}
