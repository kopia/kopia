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

func symlinkChtimes(linkPath string, atime, mtime time.Time) error {
	fta := windows.NsecToFiletime(atime.UnixNano())
	ftw := windows.NsecToFiletime(mtime.UnixNano())

	linkPath = atomicfile.MaybePrefixLongFilenameOnWindows(linkPath)

	fn, err := windows.UTF16PtrFromString(linkPath)
	if err != nil {
		return errors.Wrap(err, "UTF16PtrFromString")
	}

	h, err := windows.CreateFile(
		fn, windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return errors.Wrapf(err, "CreateFile error on %v", linkPath)
	}

	defer windows.CloseHandle(h) //nolint:errcheck

	//nolint:wrapcheck
	return windows.SetFileTime(h, &ftw, &fta, &ftw)
}
