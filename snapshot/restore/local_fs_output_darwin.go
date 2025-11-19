package restore

import (
	"os"
	"syscall"
	"time"
	"unsafe"

	"github.com/pkg/errors"
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

func symlinkChtimes(linkPath string, btime, atime, mtime time.Time) error {
	// macOS Lutimes only supports atime and mtime, birth time cannot be set on symlinks
	//nolint:wrapcheck
	return unix.Lutimes(linkPath, []unix.Timeval{
		unix.NsecToTimeval(atime.UnixNano()),
		unix.NsecToTimeval(mtime.UnixNano()),
	})
}

func chtimes(path string, btime, atime, mtime time.Time) error {
	// When btime is not available (zero time from old snapshots without btime support),
	// just use os.Chtimes to set atime/mtime and leave btime as-is (OS sets to file creation time).
	if btime.IsZero() {
		//nolint:wrapcheck
		return os.Chtimes(path, atime, mtime)
	}

	return setFileTimes(path, btime, atime, mtime)
}

// ChtimesExact is exported for testing purposes - sets times exactly as provided.
func ChtimesExact(path string, btime, atime, mtime time.Time) error {
	return setFileTimes(path, btime, atime, mtime)
}

// setFileTimes sets the birth, access, and modification times for a file on macOS.
func setFileTimes(path string, btime, atime, mtime time.Time) error {
	// First set atime and mtime using standard os.Chtimes
	if err := os.Chtimes(path, atime, mtime); err != nil {
		return errors.Wrap(err, "unable to set atime/mtime")
	}

	// Birth time setting is best-effort on macOS
	// Silently ignore errors to avoid failing restore over timestamp issues
	_ = setBirthTime(path, btime)

	return nil
}

// macOS setattrlist structures and constants
const (
	ATTR_BIT_MAP_COUNT = 5
	ATTR_CMN_CRTIME    = 0x00000200
)

type attrlist struct {
	bitmapcount uint16
	reserved    uint16
	commonattr  uint32
	volattr     uint32
	dirattr     uint32
	fileattr    uint32
	forkattr    uint32
}

func setBirthTime(path string, btime time.Time) error {
	attrs := attrlist{
		bitmapcount: ATTR_BIT_MAP_COUNT,
		commonattr:  ATTR_CMN_CRTIME,
	}

	crtime := syscall.Timespec{
		Sec:  btime.Unix(),
		Nsec: int64(btime.Nanosecond()),
	}

	pathPtr, err := unix.BytePtrFromString(path)
	if err != nil {
		return errors.Wrap(err, "unable to convert path to C string")
	}

	_, _, errno := unix.Syscall6(
		unix.SYS_SETATTRLIST,
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&attrs)),
		uintptr(unsafe.Pointer(&crtime)),
		unsafe.Sizeof(crtime),
		0, // options
		0,
	)

	if errno != 0 {
		return errors.Wrapf(errno, "setattrlist failed for %s", path)
	}

	return nil
}
