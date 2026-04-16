// Package volumesizeinfo contains helpers to obtain information about volume.
package volumesizeinfo

import (
	"github.com/pkg/errors"
)

// VolumeSizeInfo keeps information about volume (total volume size, used size and number of files).
type VolumeSizeInfo struct {
	TotalSize  uint64
	UsedSize   uint64
	FilesCount uint64
}

// GetVolumeSizeInfo returns VolumeSizeInfo for given mount point.
// FilesCount on Windows it always set to MaxInt64.
func GetVolumeSizeInfo(volumeMountPoint string) (VolumeSizeInfo, error) {
	if volumeMountPoint == "" {
		return VolumeSizeInfo{}, errors.Errorf("volume mount point cannot be empty")
	}

	sizeInfo, err := getPlatformVolumeSizeInfo(volumeMountPoint)
	if err != nil {
		return VolumeSizeInfo{}, errors.Wrapf(err, "Unable to get volume size info for mount point %q", volumeMountPoint)
	}

	return sizeInfo, nil
}
