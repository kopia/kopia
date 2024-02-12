//go:build !windows
// +build !windows

package localfs

import (
	"os"
	"path/filepath"
	"syscall"

	"github.com/pkg/errors"
	"github.com/pkg/xattr"

	"github.com/kopia/kopia/fs"
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

func platformSpecificAttributesInfo(fi os.FileInfo, prefix string) fs.AttributesInfo {
	path := filepath.Join(prefix, fi.Name())

	xattrs, err := listxattr(path)
	if err != nil {
		return nil
	}

	if len(xattrs) == 0 {
		return nil
	}

	ai := fs.AttributesInfo{}

	for _, attr := range xattrs {
		val, err := getxattr(path, attr)
		if err != nil {
			continue
		}

		ai[attr] = val
	}

	return ai
}

// getxattr retrieves extended attribute data associated with the file f.
func getxattr(path, name string) ([]byte, error) {
	b, err := xattr.LGet(path, name)
	return b, handleXattrErr(err)
}

// listxattr retrieves a list of names of extended attributes associated with the file f.
func listxattr(path string) ([]string, error) {
	l, err := xattr.LList(path)
	return l, handleXattrErr(err)
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
