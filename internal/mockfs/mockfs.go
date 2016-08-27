package mockfs

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/kopia/kopia/fs"
)

type sortedEntries fs.Entries

func (e sortedEntries) Len() int      { return len(e) }
func (e sortedEntries) Swap(i, j int) { e[i], e[j] = e[j], e[i] }
func (e sortedEntries) Less(i, j int) bool {
	return e[i].Metadata().Name < e[j].Metadata().Name
}

type entry struct {
	parent   *Directory
	metadata *fs.EntryMetadata
}

func (ime *entry) Parent() fs.Directory {
	return ime.parent
}

func (ime *entry) Metadata() *fs.EntryMetadata {
	return ime.metadata
}

// Directory is mock in-memory implementation of fs.Directory
type Directory struct {
	entry

	children     fs.Entries
	readdirError error
}

// AddFile adds a mock file with the specified name, content and permissions.
func (imd *Directory) AddFile(name string, content []byte, permissions fs.Permissions) *File {
	imd, name = imd.resolveSubdir(name)
	file := &File{
		entry: entry{
			parent: imd,
			metadata: &fs.EntryMetadata{
				Name:        name,
				Type:        fs.EntryTypeFile,
				Permissions: permissions,
			},
		},
		source: bytes.NewBuffer(content),
	}

	imd.addChild(file)

	return file
}

// AddDir adds a fake directory with a given name and permissions.
func (imd *Directory) AddDir(name string, permissions fs.Permissions) *Directory {
	imd, name = imd.resolveSubdir(name)

	subdir := &Directory{
		entry: entry{
			parent: imd,
			metadata: &fs.EntryMetadata{
				Name:        name,
				Type:        fs.EntryTypeDirectory,
				Permissions: permissions,
			},
		},
	}

	imd.addChild(subdir)

	return subdir
}

func (imd *Directory) addChild(e fs.Entry) {
	if strings.Contains(e.Metadata().Name, "/") {
		panic("child name cannot contain '/'")
	}
	imd.children = append(imd.children, e)
	sort.Sort(sortedEntries(imd.children))
}

func (imd *Directory) resolveSubdir(name string) (*Directory, string) {
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
			panic(fmt.Sprintf("'%s' not found in '%s'", n, i.metadata.Name))
		}
		if !i2.Metadata().FileMode().IsDir() {
			panic(fmt.Sprintf("'%s' is not a directory in '%s'", n, i.metadata.Name))
		}

		i = i2.(*Directory)
	}
	return i
}

// Remove removes directory entry with a given name.
func (imd *Directory) Remove(name string) {
	newChildren := imd.children[:0]

	for _, e := range imd.children {
		if e.Metadata().Name != name {
			newChildren = append(newChildren, e)
		}
	}

	imd.children = newChildren
}

// FailReaddir causes the subsequent Readdir() calls to fail with the specified error.
func (imd *Directory) FailReaddir(err error) {
	imd.readdirError = err
}

// Readdir gets the contents of a directory.
func (imd *Directory) Readdir() (fs.Entries, error) {
	if imd.readdirError != nil {
		return nil, imd.readdirError
	}

	return imd.children, nil
}

// File is an in-memory fs.File capable of simulating failures.
type File struct {
	entry

	source     io.Reader
	openError  error
	closeError error
}

type fileReader struct {
	io.Reader
	metadata   *fs.EntryMetadata
	closeError error
}

func (ifr *fileReader) EntryMetadata() (*fs.EntryMetadata, error) {
	return ifr.metadata, nil
}

func (ifr *fileReader) Close() error {
	return ifr.closeError
}

// Open opens the file for reading, optionally simulating error.
func (imf *File) Open() (fs.EntryMetadataReadCloser, error) {
	if imf.openError != nil {
		return nil, imf.openError
	}
	return &fileReader{
		Reader:     imf.source,
		metadata:   imf.metadata,
		closeError: imf.closeError,
	}, nil
}

type inmemorySymlink struct {
	entry
	target string
}

func (imsl *inmemorySymlink) Readlink() (string, error) {
	panic("not implemented yet")
}

// NewDirectory returns new mock directory.ds
func NewDirectory() *Directory {
	return &Directory{
		entry: entry{
			metadata: &fs.EntryMetadata{
				Name: "<root>",
			},
			parent: nil,
		},
	}
}

var _ fs.Directory = &Directory{}
var _ fs.File = &File{}
var _ fs.Symlink = &inmemorySymlink{}
