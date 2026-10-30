//go:build windows

package volumesizeinfo

import (
	"math"

	"golang.org/x/sys/windows"

	"github.com/kopia/kopia/repo/blob"
)

func getPlatformVolumeSizeInfo(volumeMountPoint string) (VolumeSizeInfo, error) {
	var c blob.Capacity

	pathPtr, err := windows.UTF16PtrFromString(volumeMountPoint)
	if err != nil {
		return VolumeSizeInfo{}, err //nolint:wrapcheck
	}

	err = windows.GetDiskFreeSpaceEx(pathPtr, nil, &c.SizeB, &c.FreeB)
	if err != nil {
		return VolumeSizeInfo{}, err //nolint:wrapcheck
	}

	return VolumeSizeInfo{
		TotalSize:  c.SizeB,
		UsedSize:   c.SizeB - c.FreeB,
		FilesCount: uint64(math.MaxInt64), // On Windows it's not possible to get / estimate number of files on volume
	}, nil
}
