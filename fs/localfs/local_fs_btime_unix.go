//go:build !windows && !darwin && !freebsd && !linux

package localfs

import (
	"syscall"
)

func platformSpecificBirthTimeFromStat(_ *syscall.Stat_t, path string) int64 {
	// Birth time not supported on this platform
	return 0
}
