// Package mockfs implements in-memory filesystem for testing.
package mockfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/kopia/kopia/fs"
)

// ReaderSeekerCloser implements io.Reader, io.Seeker and io.Closer
type ReaderSeekerCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

type readerSeekerCloser struct {
	io.ReadSeeker
}

func (c readerSeekerCloser) Close() error {
	return nil
}

type entry struct {
	name    string
	mode    os.FileMode
	size    int64
	modTime time.Time
	owner   fs.OwnerInfo
}

func (e entry) Name() string {
	return e.name
}

func (e entry) IsDir() bool {
	return e.mode.IsDir()
}

func (e entry) Mode() os.FileMode {
	return e.mode
}

func (e entry) ModTime() time.Time {
	return e.modTime
}

func (e entry) Size() int64 {
	return e.size
}

func (e entry) Sys() interface{} {
	return nil
}

func (e entry) Owner() fs.OwnerInfo {
	return e.owner
}

// Directory is mock in-memory implementation of fs.Directory
type Directory struct {
	entry

	children     fs.Entries
	readdirError error
	onReaddir    func()
}

// Summary returns summary of a directory.
func (imd *Directory) Summary() *fs.DirectorySummary {
	return nil
}

// AddFileLines adds a mock file with the specified name, text content and permissions.
func (imd *Directory) AddFileLines(name string, lines []string, permissions os.FileMode) *File {
	return imd.AddFile(name, []byte(strings.Join(lines, "\n")), permissions)
}

// AddFile adds a mock file with the specified name, content and permissions.
func (imd *Directory) AddFile(name string, content []byte, permissions os.FileMode) *File {
	imd, name = imd.resolveSubdir(name)
	file := &File{
		entry: entry{
			name: name,
			mode: permissions,
			size: int64(len(content)),
		},
		source: func() (ReaderSeekerCloser, error) {
			return readerSeekerCloser{bytes.NewReader(content)}, nil
		},
	}

	imd.addChild(file)

	return file
}

// AddDir adds a fake directory with a given name and permissions.
func (imd *Directory) AddDir(name string, permissions os.FileMode) *Directory {
	imd, name = imd.resolveSubdir(name)

	subdir := &Directory{
		entry: entry{
			name: name,
			mode: permissions | os.ModeDir,
		},
	}

	imd.addChild(subdir)

	return subdir
}

func (imd *Directory) addChild(e fs.Entry) {
	if strings.Contains(e.Name(), "/") {
		panic("child name cannot contain '/'")
	}

	imd.children = append(imd.children, e)
	imd.children.Sort()
}

func (imd *Directory) resolveSubdir(name string) (parent *Directory, leaf string) {
	parts := strings.Split(name, "/")
	for _, n := range parts[0 : len(parts)-1] {
		imd = imd.Subdir(n)
	}

	return imd, parts[len(parts)-1]
}

// Subdir finds a subdirectory with a given name.
func (imd *Directory) Subdir(name ...string) *Directory {
	i := imd

	for _, n := range name {
		i2 := i.children.FindByName(n)
		if i2 == nil {
			panic(fmt.Sprintf("'%s' not found in '%s'", n, i.Name()))
		}

		if !i2.IsDir() {
			panic(fmt.Sprintf("'%s' is not a directory in '%s'", n, i.Name()))
		}

		i = i2.(*Directory)
	}

	return i
}

// Remove removes directory entry with a given name.
func (imd *Directory) Remove(name string) {
	newChildren := imd.children[:0]

	for _, e := range imd.children {
		if e.Name() != name {
			newChildren = append(newChildren, e)
		}
	}

	imd.children = newChildren
}

// FailReaddir causes the subsequent Readdir() calls to fail with the specified error.
func (imd *Directory) FailReaddir(err error) {
	imd.readdirError = err
}

// OnReaddir invokes the provided function on read.
func (imd *Directory) OnReaddir(cb func()) {
	imd.onReaddir = cb
}

// Child gets the named child of a directory.
func (imd *Directory) Child(ctx context.Context, name string) (fs.Entry, error) {
	return fs.ReadDirAndFindChild(ctx, imd, name)
}

// Readdir gets the contents of a directory.
func (imd *Directory) Readdir(ctx context.Context) (fs.Entries, error) {
	if imd.readdirError != nil {
		return nil, imd.readdirError
	}

	if imd.onReaddir != nil {
		imd.onReaddir()
	}

	return append(fs.Entries(nil), imd.children...), nil
}

// File is an in-memory fs.File capable of simulating failures.
type File struct {
	entry

	source func() (ReaderSeekerCloser, error)
}

// SetContents changes the contents of a given file.
func (imf *File) SetContents(b []byte) {
	imf.source = func() (ReaderSeekerCloser, error) {
		return readerSeekerCloser{bytes.NewReader(b)}, nil
	}
}

type fileReader struct {
	ReaderSeekerCloser
	entry fs.Entry
}

func (ifr *fileReader) Entry() (fs.Entry, error) {
	return ifr.entry, nil
}

// Open opens the file for reading, optionally simulating error.
func (imf *File) Open(ctx context.Context) (fs.Reader, error) {
	r, err := imf.source()
	if err != nil {
		return nil, err
	}

	return &fileReader{
		ReaderSeekerCloser: r,
		entry:              imf,
	}, nil
}

type inmemorySymlink struct {
	entry
}

func (imsl *inmemorySymlink) Readlink(ctx context.Context) (string, error) {
	panic("not implemented yet")
}

// NewDirectory returns new mock directory.
func NewDirectory() *Directory {
	return &Directory{
		entry: entry{
			name: "<root>",
			mode: 0777 | os.ModeDir, // nolint:gomnd
		},
	}
}

var (
	_ fs.Directory = &Directory{}
	_ fs.File      = &File{}
	_ fs.Symlink   = &inmemorySymlink{}
)
