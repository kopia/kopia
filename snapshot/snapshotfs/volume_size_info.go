package snapshotfs

import (
	"github.com/pkg/errors"
)

type volumeSizeInfo struct {
	totalSize  uint64
	usedSize   uint64
	filesCount uint64
}

func getVolumeSizeInfo(volumeMountPoint string) (volumeSizeInfo, error) {
	if volumeMountPoint == "" {
		return volumeSizeInfo{}, errors.Errorf("volume mount point cannot be empty")
	}

	sizeInfo, err := getPlatformVolumeSizeInfo(volumeMountPoint)
	if err != nil {
		return volumeSizeInfo{}, errors.Wrapf(err, "Unable to get volume size info for mount point %q", volumeMountPoint)
	}

	return sizeInfo, nil
}
