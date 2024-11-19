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

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
)

// DefaultModTime is the default modification time for mock filesystem entries.
//
//nolint:gochecknoglobals
var DefaultModTime = time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC)

// ReaderSeekerCloser implements io.Reader, io.Seeker and io.Closer.
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
	device  fs.DeviceInfo
}

func (e *entry) Name() string {
	return e.name
}

func (e *entry) IsDir() bool {
	return e.mode.IsDir()
}

func (e *entry) Mode() os.FileMode {
	return e.mode
}

func (e *entry) ModTime() time.Time {
	return e.modTime
}

func (e *entry) Size() int64 {
	return e.size
}

func (e *entry) Sys() interface{} {
	return nil
}

func (e *entry) Owner() fs.OwnerInfo {
	return e.owner
}

func (e *entry) Device() fs.DeviceInfo {
	return e.device
}

func (e *entry) LocalFilesystemPath() string {
	return ""
}

func (e *entry) Close() {
}

// Directory is mock in-memory implementation of fs.Directory.
type Directory struct {
	entry

	parent       *Directory
	children     []fs.Entry
	readdirError error
	onReaddir    func()
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
			name:    name,
			mode:    permissions,
			size:    int64(len(content)),
			modTime: DefaultModTime,
		},
		source: func() (ReaderSeekerCloser, error) {
			return readerSeekerCloser{bytes.NewReader(content)}, nil
		},
	}

	imd.addChild(file)

	return file
}

// AddFileWithSource adds a mock file with the specified name, permissions, and
// given source function for getting a Reader instance.
func (imd *Directory) AddFileWithSource(name string, permissions os.FileMode, source func() (ReaderSeekerCloser, error)) *File {
	imd, name = imd.resolveSubdir(name)
	file := &File{
		entry: entry{
			name:    name,
			mode:    permissions,
			size:    0,
			modTime: DefaultModTime,
		},
		source: source,
	}

	imd.addChild(file)

	return file
}

func (imd *Directory) getRoot() *Directory {
	root := imd
	for root.parent != nil {
		root = root.parent
	}

	return root
}

// AddSymlink adds a mock symlink with the specified name, target and permissions.
func (imd *Directory) AddSymlink(name, target string, permissions os.FileMode) *Symlink {
	imd, name = imd.resolveSubdir(name)
	sl := &Symlink{
		entry: entry{
			name:    name,
			mode:    permissions | os.ModeSymlink,
			size:    int64(len(target)),
			modTime: DefaultModTime,
		},
		parent: imd,
		target: target,
	}

	imd.addChild(sl)

	return sl
}

// AddFileDevice adds a mock file with the specified name, content, permissions, and device info.
func (imd *Directory) AddFileDevice(name string, content []byte, permissions os.FileMode, deviceInfo fs.DeviceInfo) *File {
	imd, name = imd.resolveSubdir(name)
	file := &File{
		entry: entry{
			name:    name,
			mode:    permissions,
			size:    int64(len(content)),
			device:  deviceInfo,
			modTime: DefaultModTime,
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
			name:    name,
			mode:    permissions | os.ModeDir,
			modTime: DefaultModTime,
		},
		parent: imd,
	}

	imd.addChild(subdir)

	return subdir
}

// AddErrorEntry adds a fake directory with a given name and permissions.
func (imd *Directory) AddErrorEntry(name string, permissions os.FileMode, err error) *ErrorEntry {
	imd, name = imd.resolveSubdir(name)

	ee := &ErrorEntry{
		entry: entry{
			name:    name,
			mode:    permissions | os.ModeDir,
			modTime: DefaultModTime,
		},
		err: err,
	}

	imd.addChild(ee)

	return ee
}

// AddDirDevice adds a fake directory with a given name and permissions.
func (imd *Directory) AddDirDevice(name string, permissions os.FileMode, deviceInfo fs.DeviceInfo) *Directory {
	imd, name = imd.resolveSubdir(name)

	subdir := &Directory{
		entry: entry{
			name:    name,
			mode:    permissions | os.ModeDir,
			device:  deviceInfo,
			modTime: DefaultModTime,
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
	fs.Sort(imd.children)
}

func (imd *Directory) resolveSubdir(name string) (parent *Directory, leaf string) {
	parts := strings.Split(name, "/")
	for _, n := range parts[0 : len(parts)-1] {
		switch n {
		case ".", "":
			continue
		case "..":
			imd = imd.parent
		default:
			imd = imd.Subdir(n)
		}
	}

	return imd, parts[len(parts)-1]
}

// Subdir finds a subdirectory with a given name.
func (imd *Directory) Subdir(name ...string) *Directory {
	i := imd

	for _, n := range name {
		i2 := fs.FindByName(i.children, n)
		if i2 == nil {
			panic(fmt.Sprintf("'%s' not found in '%s'", n, i.Name()))
		}

		if !i2.IsDir() {
			panic(fmt.Sprintf("'%s' is not a directory in '%s'", n, i.Name()))
		}

		//nolint:forcetypeassert
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

// FailReaddir causes the subsequent IterateEntries() calls to fail with the specified error.
func (imd *Directory) FailReaddir(err error) {
	imd.readdirError = err
}

// OnReaddir invokes the provided function on read.
func (imd *Directory) OnReaddir(cb func()) {
	imd.onReaddir = cb
}

// SupportsMultipleIterations returns whether this directory can be iterated through multiple times.
func (imd *Directory) SupportsMultipleIterations() bool {
	return true
}

// Child gets the named child of a directory.
func (imd *Directory) Child(ctx context.Context, name string) (fs.Entry, error) {
	e := fs.FindByName(imd.children, name)
	if e != nil {
		return e, nil
	}

	return nil, fs.ErrEntryNotFound
}

// Iterate returns directory iterator.
func (imd *Directory) Iterate(ctx context.Context) (fs.DirectoryIterator, error) {
	if imd.readdirError != nil {
		return nil, errors.Wrapf(imd.readdirError, "in mockfs Directory.Iterate on directory %s", imd.name)
	}

	if imd.onReaddir != nil {
		imd.onReaddir()
	}

	return fs.StaticIterator(append([]fs.Entry{}, imd.children...), nil), nil
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

// Symlink is a mock implementation of the fs.Symlink interface.
type Symlink struct {
	entry

	parent *Directory
	target string
}

// Resolve implements fs.Symlink interface.
func (imsl *Symlink) Resolve(ctx context.Context) (fs.Entry, error) {
	dir := imsl.parent

	// Mockfs uses Unix path separators
	if imsl.target[0] == '/' {
		// Absolute link
		dir = dir.getRoot()
	}

	dir, name := dir.resolveSubdir(imsl.target)
	target, err := dir.Child(ctx, name)

	return target, err
}

// Readlink implements fs.Symlink interface.
func (imsl *Symlink) Readlink(ctx context.Context) (string, error) {
	return imsl.target, nil
}

// NewDirectory returns new mock directory.
func NewDirectory() *Directory {
	return &Directory{
		entry: entry{
			name:    "<root>",
			mode:    0o777 | os.ModeDir, //nolint:mnd
			modTime: DefaultModTime,
		},
	}
}

// NewFile returns a new mock file with the given name, contents, and mode.
func NewFile(name string, content []byte, permissions os.FileMode) *File {
	return &File{
		entry: entry{
			name:    name,
			mode:    permissions,
			size:    int64(len(content)),
			modTime: DefaultModTime,
		},
		source: func() (ReaderSeekerCloser, error) {
			return readerSeekerCloser{bytes.NewReader(content)}, nil
		},
	}
}

// ErrorEntry is mock in-memory implementation of fs.ErrorEntry.
type ErrorEntry struct {
	entry
	err error
}

// ErrorInfo implements fs.ErrorErntry.
func (e *ErrorEntry) ErrorInfo() error {
	return e.err
}

var (
	_ fs.Directory  = &Directory{}
	_ fs.File       = &File{}
	_ fs.Symlink    = &Symlink{}
	_ fs.ErrorEntry = &ErrorEntry{}
)
