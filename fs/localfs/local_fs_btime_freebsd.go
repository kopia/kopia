package localfs

import (
	"syscall"
)

func platformSpecificBirthTimeFromStat(stat *syscall.Stat_t, _ string) int64 {
	// FreeBSD has Birthtimespec field (similar to macOS)
	return stat.Birthtimespec.Sec*int64(1e9) + int64(stat.Birthtimespec.Nsec)
}
