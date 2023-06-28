//go:build linux || freebsd || openbsd
// +build linux freebsd openbsd

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

func symlinkChtimes(linkPath string, atime, mtime time.Time) error {
	//nolint:wrapcheck
	return unix.Lutimes(linkPath, []unix.Timeval{
		unix.NsecToTimeval(atime.UnixNano()),
		unix.NsecToTimeval(mtime.UnixNano()),
	})
}
