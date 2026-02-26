package localfs

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// platformSpecificBirthTimeFromStat retrieves birth time using statx(2).
// Requires Linux kernel 4.11+ and filesystem support (e.g., ext4 with btime, btrfs, xfs).
// Returns 0 if birth time is unavailable (older kernels, unsupported filesystems like ext3).
func platformSpecificBirthTimeFromStat(_ *syscall.Stat_t, path string) int64 {
	// Linux doesn't have birth time in syscall.Stat_t
	var statx unix.Statx_t

	err := unix.Statx(unix.AT_FDCWD, path, unix.AT_SYMLINK_NOFOLLOW, unix.STATX_BTIME, &statx)
	if err != nil {
		// statx might fail on older kernels (< 4.11) or filesystems that don't support it
		return 0
	}

	if statx.Mask&unix.STATX_BTIME == 0 {
		// Filesystem doesn't support birth time (e.g., ext3, older ext4)
		return 0
	}

	// Convert statx timestamp to nanoseconds
	return statx.Btime.Sec*1e9 + int64(statx.Btime.Nsec)
}
