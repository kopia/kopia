//go:build !windows
// +build !windows

package localfs

import (
	"os"
	"syscall"

	"github.com/kopia/kopia/fs"
	"golang.org/x/sys/unix"
)

func platformSpecificOwnerInfo(fi os.FileInfo) fs.OwnerInfo {
	var oi fs.OwnerInfo
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		oi.UserID = stat.Uid
		oi.GroupID = stat.Gid
	}

	return oi
}

func platformSpecificDeviceInfo(fi os.FileInfo) fs.DeviceInfo {
	var oi fs.DeviceInfo
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		// not making a separate type for 32-bit platforms here..
		oi.Dev = platformSpecificWidenDev(stat.Dev)
		oi.Rdev = platformSpecificWidenDev(stat.Rdev)
	}

	return oi
}

func platformSpecificNewEntry(fi os.FileInfo, prefix string) filesystemEntry {
	var uId uint64
	if stat, ok := fi.Sys().(*unix.Stat_t); ok {
		uId = stat.Ino
	}
	return filesystemEntry{
		TrimShallowSuffix(fi.Name()),
		fi.Size(),
		fi.ModTime().UnixNano(),
		fi.Mode(),
		platformSpecificOwnerInfo(fi),
		platformSpecificDeviceInfo(fi),
		uId,
		prefix,
	}
}
