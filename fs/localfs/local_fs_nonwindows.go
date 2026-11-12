//go:build !windows

package localfs

import (
	"os"
	"syscall"

	"github.com/kopia/kopia/fs"
)

const isWindows = false

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

func platformSpecificHardLinkInfo(fi os.FileInfo) fs.HardLinkInfo {
	var hli fs.HardLinkInfo
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		hli.UniqID = stat.Ino
		hli.NLink = widenNlink(stat.Nlink)
	}

	return hli
}

// widenNlink converts Stat_t.Nlink, whose width varies by platform
// (uint16 on darwin, uint32 on linux/arm64 and the BSDs, uint64 on linux/amd64), to uint64.
func widenNlink[T ~uint16 | ~uint32 | ~uint64](n T) uint64 {
	return uint64(n)
}

// Direct Windows volume paths (e.g. Shadow Copy) require a trailing separator.
// The non-windows implementation can be optimized away by the compiler.
func trailingSeparator(_ *filesystemDirectory) string {
	return ""
}
