//go:build !windows && !darwin && !freebsd && !linux

package localfs

import (
	"syscall"
)

// platformSpecificBirthTimeFromStat is a fallback for Unix systems without birth time support.
// This applies to: OpenBSD, NetBSD, Solaris, AIX, and other Unix-like systems.
func platformSpecificBirthTimeFromStat(_ *syscall.Stat_t, path string) int64 {
	// Birth time not supported on this platform
	return 0
}
