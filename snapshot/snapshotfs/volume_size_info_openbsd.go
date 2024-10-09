//go:build openbsd

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
		totalSize:  stats.F_blocks * uint64(stats.F_bsize),
		usedSize:   (stats.F_blocks - stats.F_bfree) * uint64(stats.F_bsize),
		filesCount: stats.F_files - stats.F_ffree,
	}, nil
}
