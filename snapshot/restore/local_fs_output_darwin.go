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

func symlinkChxattr(path string, ai fs.AttributesInfo) error {
	for name, data := range ai {
		if err := handleXattrErr(xattr.Set(path, name, data)); err != nil {
			return err
		}
	}

	return nil
}

func chxattr(path string, ai fs.AttributesInfo) error {
	for name, data := range ai {
		if err := handleXattrErr(xattr.LSet(path, name, data)); err != nil {
			return err
		}
	}

	return nil
}

func handleXattrErr(err error) error {
	// TODO(miek): duplicated in fs/localfs/local_fs_nonwindows.go
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
