package restore

import (
	"os"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sys/windows"

	"github.com/kopia/kopia/internal/atomicfile"
)

//nolint:revive
func symlinkChown(path string, uid, gid int) error {
	return nil
}

//nolint:revive
func symlinkChmod(path string, mode os.FileMode) error {
	return nil
}

func symlinkChtimes(linkPath string, btime, atime, mtime time.Time) error {
	return setFileTimes(linkPath, btime, atime, mtime, true)
}

func chtimes(path string, btime, atime, mtime time.Time) error {
	// When btime is not available (zero time from old snapshots without btime support),
	// just use os.Chtimes to set atime/mtime and leave btime as-is (OS sets to file creation time).
	if btime.IsZero() {
		//nolint:wrapcheck
		return os.Chtimes(path, atime, mtime)
	}

	return setFileTimes(path, btime, atime, mtime, false)
}

// ChtimesExact is exported for testing purposes - sets times exactly as provided without fallback.
func ChtimesExact(path string, btime, atime, mtime time.Time) error {
	return setFileTimes(path, btime, atime, mtime, false)
}

// setFileTimes sets the creation, access, and modification times for a file or symlink on Windows.
func setFileTimes(path string, btime, atime, mtime time.Time, isSymlink bool) error {
	// Convert times to Windows FILETIME format
	ftc := windows.NsecToFiletime(btime.UnixNano())
	fta := windows.NsecToFiletime(atime.UnixNano())
	ftw := windows.NsecToFiletime(mtime.UnixNano())

	path = atomicfile.MaybePrefixLongFilenameOnWindows(path)

	fn, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return errors.Wrap(err, "UTF16PtrFromString")
	}

	var access uint32
	var flags uint32
	if isSymlink {
		access = windows.GENERIC_READ | windows.GENERIC_WRITE
		flags = windows.FILE_FLAG_OPEN_REPARSE_POINT
	} else {
		access = windows.FILE_WRITE_ATTRIBUTES
		flags = windows.FILE_FLAG_BACKUP_SEMANTICS
	}

	h, err := windows.CreateFile(
		fn, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil, windows.OPEN_EXISTING,
		flags, 0)
	if err != nil {
		return errors.Wrapf(err, "CreateFile error on %v", path)
	}

	defer windows.CloseHandle(h) //nolint:errcheck

	//nolint:wrapcheck
	return windows.SetFileTime(h, &ftc, &fta, &ftw)
}
