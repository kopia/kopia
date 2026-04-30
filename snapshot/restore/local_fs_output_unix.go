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

func symlinkChtimes(linkPath string, _, atime, mtime time.Time) error {
	// Unix Lutimes only supports atime and mtime, birth time cannot be set on symlinks
	//nolint:wrapcheck
	return unix.Lutimes(linkPath, []unix.Timeval{
		unix.NsecToTimeval(atime.UnixNano()),
		unix.NsecToTimeval(mtime.UnixNano()),
	})
}

func chtimes(path string, _, atime, mtime time.Time) error {
	// On Unix-like systems (Linux, FreeBSD, OpenBSD, etc.), birth time cannot be set after file creation.
	// macOS has its own implementation in local_fs_output_darwin.go, which handles birth time differently.
	// The birthtime stored in snapshots is still valuable for:
	// 1. Cross-platform restore (e.g., Linux snapshot -> macOS/Windows restore)
	// 2. Future kernel support for birthtime setting
	// 3. Consistent metadata model across all platforms
	//
	// When restoring on Unix-like systems, birthtime will be set to file creation time (now).
	// This is consistent with standard Unix filesystem behavior.
	//nolint:wrapcheck
	return os.Chtimes(path, atime, mtime)
}

// ChtimesExact is exported for testing purposes.
// On Unix, birth time cannot be set, so this only sets atime and mtime.
func ChtimesExact(path string, _, atime, mtime time.Time) error {
	//nolint:wrapcheck
	return os.Chtimes(path, atime, mtime)
}
