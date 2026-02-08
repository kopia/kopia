package localfs

import (
	"syscall"
)

func platformSpecificBirthTimeFromStat(stat *syscall.Stat_t, _ string) int64 {
	// macOS has Birthtimespec field
	return stat.Birthtimespec.Sec*int64(1e9) + int64(stat.Birthtimespec.Nsec)
}
