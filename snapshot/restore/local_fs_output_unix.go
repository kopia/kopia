//go:build linux || freebsd || openbsd
// +build linux freebsd openbsd

package restore

import (
	"os"
	"syscall"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/pkg/errors"
	"github.com/pkg/xattr"
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

func chXattr(path string, ai fs.AttributesInfo) error {
	for name, data := range ai {
		if err := setxattr(path, name, data); err != nil {
			return err
		}
	}
	return nil
}

// setxattr associates name and data together as an attribute of path.
func setxattr(path, name string, data []byte) error {
	return handleXattrErr(xattr.LSet(path, name, data))
}

func handleXattrErr(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, xattr.ENOATTR) {
		return nil
	}

	if errors.Is(err, syscall.ENOTSUP) {
		return nil
	}

	return err
}
