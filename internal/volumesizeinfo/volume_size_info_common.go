//go:build !openbsd && !windows

package volumesizeinfo

import (
	"golang.org/x/sys/unix"
)

func getPlatformVolumeSizeInfo(volumeMountPoint string) (VolumeSizeInfo, error) {
	stats := unix.Statfs_t{}

	err := unix.Statfs(volumeMountPoint, &stats)
	if err != nil {
		return VolumeSizeInfo{}, err //nolint:wrapcheck
	}

	return VolumeSizeInfo{
		TotalSize: stats.Blocks * uint64(stats.Bsize),                 //nolint:gosec,unconvert,nolintlint
		UsedSize:  (stats.Blocks - stats.Bfree) * uint64(stats.Bsize), //nolint:gosec,unconvert,nolintlint
		// Conversion to uint64 is needed for some arch/distrib combination.
		FilesCount: stats.Files - uint64(stats.Ffree), //nolint:unconvert,nolintlint
	}, nil
}
