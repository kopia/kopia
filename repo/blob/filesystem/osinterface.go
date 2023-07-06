package filesystem

import (
	"io"
	"io/fs"
	"os"
	"time"
)

// osInterface is an operating system file interface, used by filesystemStorage to support mocking.
//
//nolint:interfacebloat
type osInterface interface {
	Open(fname string) (osReadFile, error)
	IsNotExist(err error) bool
	IsExist(err error) bool
	IsPathError(err error) bool
	IsLinkError(err error) bool
	IsPathSeparator(c byte) bool
	IsStale(err error) bool
	Remove(fname string) error
	Rename(oldname, newname string) error
	ReadDir(dirname string) ([]fs.DirEntry, error)
	Stat(fname string) (os.FileInfo, error)
	CreateNewFile(fname string, mode os.FileMode) (osWriteFile, error)
	Mkdir(fname string, mode os.FileMode) error
	MkdirAll(fname string, mode os.FileMode) error
	Chtimes(fname string, atime, mtime time.Time) error
	Geteuid() int
	Chown(fname string, uid, gid int) error
}

type osReadFile interface {
	io.ReadSeekCloser

	Stat() (os.FileInfo, error)
}

type osWriteFile interface {
	io.WriteCloser
}
