//go:build openbsd

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
		TotalSize:  stats.F_blocks * uint64(stats.F_bsize),
		UsedSize:   (stats.F_blocks - stats.F_bfree) * uint64(stats.F_bsize),
		FilesCount: stats.F_files - stats.F_ffree,
	}, nil
}
