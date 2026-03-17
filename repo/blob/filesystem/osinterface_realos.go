package filesystem

import (
	"errors"
	"io/fs"
	"os"
	"time"

	"github.com/kopia/kopia/internal/atomicfile"
)

// realOS is an implementation of osInterface that uses real operating system calls.
type realOS struct{}

// longPath applies MaybePrefixLongFilenameOnWindows to handle paths exceeding
// MAX_PATH on Windows. On non-Windows platforms this is a no-op.
func longPath(p string) string {
	return atomicfile.MaybePrefixLongFilenameOnWindows(p)
}

func (realOS) Open(fname string) (osReadFile, error) {
	f, err := os.Open(longPath(fname)) //nolint:gosec
	if err != nil {
		//nolint:wrapcheck
		return nil, err
	}

	return f, nil
}

func (realOS) IsNotExist(err error) bool { return os.IsNotExist(err) }

func (realOS) IsExist(err error) bool { return os.IsExist(err) }

func (realOS) IsPathSeparator(c byte) bool { return os.IsPathSeparator(c) }

func (realOS) Rename(oldname, newname string) error {
	//nolint:wrapcheck
	return os.Rename(longPath(oldname), longPath(newname))
}

func (realOS) ReadDir(dirname string) ([]fs.DirEntry, error) {
	//nolint:wrapcheck
	return os.ReadDir(longPath(dirname))
}

func (realOS) IsPathError(err error) bool {
	var pe *os.PathError

	return errors.As(err, &pe)
}

func (realOS) IsLinkError(err error) bool {
	var pe *os.LinkError

	return errors.As(err, &pe)
}

func (realOS) Remove(fname string) error {
	//nolint:wrapcheck
	return os.Remove(longPath(fname))
}

func (realOS) Stat(fname string) (os.FileInfo, error) {
	//nolint:wrapcheck
	return os.Stat(longPath(fname))
}

func (realOS) CreateNewFile(fname string, perm os.FileMode) (osWriteFile, error) {
	//nolint:wrapcheck,gosec
	return os.OpenFile(longPath(fname), os.O_CREATE|os.O_EXCL|os.O_WRONLY, perm)
}

func (realOS) Mkdir(fname string, mode os.FileMode) error {
	//nolint:wrapcheck
	return os.Mkdir(longPath(fname), mode)
}

func (realOS) MkdirAll(fname string, mode os.FileMode) error {
	//nolint:wrapcheck
	return os.MkdirAll(longPath(fname), mode)
}

func (realOS) Chtimes(fname string, atime, mtime time.Time) error {
	//nolint:wrapcheck
	return os.Chtimes(longPath(fname), atime, mtime)
}

func (realOS) Geteuid() int {
	return os.Geteuid()
}

func (realOS) Chown(fname string, uid, gid int) error {
	//nolint:wrapcheck
	return os.Chown(longPath(fname), uid, gid)
}

var _ osInterface = realOS{}
