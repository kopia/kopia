//go:build !openbsd && !windows

package snapshotfs

import (
	"golang.org/x/sys/unix"
)

func getPlatformVolumeSizeInfo(volumeMountPoint string) (volumeSizeInfo, error) {
	stats := unix.Statfs_t{}

	err := unix.Statfs(volumeMountPoint, &stats)
	if err != nil {
		return volumeSizeInfo{}, err //nolint:wrapcheck
	}

	return volumeSizeInfo{
		totalSize: stats.Blocks * uint64(stats.Bsize),
		usedSize:  (stats.Blocks - stats.Bfree) * uint64(stats.Bsize),
		// Conversion to uint64 is needed for some arch/distrib combination.
		filesCount: stats.Files - uint64(stats.Ffree), //nolint:unconvert
	}, nil
}
