//go:build !windows
// +build !windows

package localfs

import (
	"os"
	"runtime"
	"syscall"

	"github.com/kopia/kopia/fs"
	"golang.org/x/sys/unix"
)

const isWindows = false

func platformSpecificOwnerInfo(fi os.FileInfo) fs.OwnerInfo {
	var oi fs.OwnerInfo
	if stat, ok := fi.Sys().(*unix.Stat_t); ok {
		oi.UserID = stat.Uid
		oi.GroupID = stat.Gid
	}

	return oi
}

func platformSpecificDeviceInfo(fi os.FileInfo) fs.DeviceInfo {
	var di fs.DeviceInfo

	switch runtime.GOOS {
	case "linux":
		if stat, ok := fi.Sys().(*unix.Stat_t); ok {
			di.Dev = platformSpecificWidenDev(stat.Dev)
			di.Rdev = platformSpecificWidenDev(stat.Rdev)
		}
	default:
		if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
			di.Dev = platformSpecificWidenDev(stat.Dev)
			di.Rdev = platformSpecificWidenDev(stat.Rdev)
		}
	}

	return di
}

func platformSpecificHardLinkInfo(fi os.FileInfo) fs.HardLinkInfo {
	var hli fs.HardLinkInfo

	switch runtime.GOOS {
	case "linux":
		if stat, ok := fi.Sys().(*unix.Stat_t); ok {
			hli.UniqId = stat.Ino
			hli.NLink = uint64(stat.Nlink)
		}
	default:
		if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
			hli.UniqId = stat.Ino
			hli.NLink = uint64(stat.Nlink)
		}
	}

	return hli
}

func platformSpecificNewEntry(basename string, fi os.FileInfo, prefix string) filesystemEntry {
	return filesystemEntry{
		TrimShallowSuffix(basename),
		fi.Size(),
		fi.ModTime().UnixNano(),
		fi.Mode(),
		platformSpecificOwnerInfo(fi),
		platformSpecificDeviceInfo(fi),
		platformSpecificHardLinkInfo(fi),
		prefix,
	}
}

// Direct Windows volume paths (e.g. Shadow Copy) require a trailing separator.
// The non-windows implementation can be optimized away by the compiler.
func trailingSeparator(_ *filesystemDirectory) string {
	return ""
}
