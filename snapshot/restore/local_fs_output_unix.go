//go:build linux || freebsd || openbsd

package restore

import (
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func symlinkChown(path string, uid, gid int) error {
	//nolint:wrapcheck
	return unix.Lchown(path, uid, gid)
}

//nolint:revive
func symlinkChmod(path string, mode os.FileMode) error {
	// linux does not support permissions on symlinks
	return nil
}

func symlinkChtimes(linkPath string, btime, atime, mtime time.Time) error {
	// Unix Lutimes only supports atime and mtime, birth time cannot be set on symlinks
	//nolint:wrapcheck
	return unix.Lutimes(linkPath, []unix.Timeval{
		unix.NsecToTimeval(atime.UnixNano()),
		unix.NsecToTimeval(mtime.UnixNano()),
	})
}

func chtimes(path string, btime, atime, mtime time.Time) error {
	// On Linux/FreeBSD, birth time cannot be set after file creation.
	// The birthtime stored in snapshots is still valuable for:
	// 1. Cross-platform restore (e.g., Linux snapshot -> macOS/Windows restore)
	// 2. Future Linux kernel support for birthtime setting
	// 3. Consistent metadata model across all platforms
	//
	// When restoring on Linux, birthtime will be set to file creation time (now).
	// This is consistent with standard Linux filesystem behavior.
	//nolint:wrapcheck
	return os.Chtimes(path, atime, mtime)
}

// ChtimesExact is exported for testing purposes.
// On Unix, birth time cannot be set, so this behaves the same as chtimes.
func ChtimesExact(path string, btime, atime, mtime time.Time) error {
	return chtimes(path, btime, atime, mtime)
}
