package restore

import (
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func symlinkChown(path string, uid, gid int) error {
	return unix.Lchown(path, uid, gid)
}

func symlinkChmod(path string, mode os.FileMode) error {
	// linux does not support permissions on symlinks
	return nil
}

func symlinkChtimes(linkPath string, atime, mtime time.Time) error {
	return unix.Lutimes(linkPath, []unix.Timeval{
		unix.NsecToTimeval(atime.UnixNano()),
		unix.NsecToTimeval(mtime.UnixNano()),
	})
}
