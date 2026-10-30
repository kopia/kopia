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

func symlinkChmod(path string, mode os.FileMode) error {
	//nolint:wrapcheck
	return unix.Fchmodat(unix.AT_FDCWD, path, uint32(mode), unix.AT_SYMLINK_NOFOLLOW)
}

func symlinkChtimes(linkPath string, atime, mtime time.Time) error {
	//nolint:wrapcheck
	return unix.Lutimes(linkPath, []unix.Timeval{
		unix.NsecToTimeval(atime.UnixNano()),
		unix.NsecToTimeval(mtime.UnixNano()),
	})
}
