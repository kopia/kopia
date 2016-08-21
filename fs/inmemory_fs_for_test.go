package fs

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
)

type inmemoryEntry struct {
	parent   *inmemoryDirectory
	metadata *EntryMetadata
}

func (ime *inmemoryEntry) Parent() Directory {
	return ime.parent
}

func (ime *inmemoryEntry) Metadata() *EntryMetadata {
	return ime.metadata
}

type inmemoryDirectory struct {
	inmemoryEntry

	children     Entries
	readdirError error
}

func (imd *inmemoryDirectory) addFile(name string, content []byte, permissions Permissions) *inmemoryFile {
	imd, name = imd.resolveSubdir(name)
	file := &inmemoryFile{
		inmemoryEntry: inmemoryEntry{
			parent: imd,
			metadata: &EntryMetadata{
				Name:        name,
				Type:        EntryTypeFile,
				Permissions: permissions,
			},
		},
		source: bytes.NewBuffer(content),
	}

	imd.addChild(file)

	return file
}

func (imd *inmemoryDirectory) addDir(name string, permissions Permissions) *inmemoryDirectory {
	imd, name = imd.resolveSubdir(name)

	subdir := &inmemoryDirectory{
		inmemoryEntry: inmemoryEntry{
			parent: imd,
			metadata: &EntryMetadata{
				Name:        name,
				Type:        EntryTypeDirectory,
				Permissions: permissions,
			},
		},
	}

	imd.addChild(subdir)

	return subdir
}

func (imd *inmemoryDirectory) addChild(e Entry) {
	if strings.Contains(e.Metadata().Name, "/") {
		panic("child name cannot contain '/'")
	}
	imd.children = append(imd.children, e)
	sort.Sort(imd.children)
}

func (imd *inmemoryDirectory) resolveSubdir(name string) (*inmemoryDirectory, string) {
	parts := strings.Split(name, "/")
	for _, n := range parts[0 : len(parts)-1] {
		imd = imd.subdir(n)
	}
	return imd, parts[len(parts)-1]
}

func (imd *inmemoryDirectory) subdir(name ...string) *inmemoryDirectory {
	i := imd
	for _, n := range name {
		i2 := i.children.FindByName(n)
		if i2 == nil {
			panic(fmt.Sprintf("'%s' not found in '%s'", n, i.metadata.Name))
		}
		if !i2.Metadata().FileMode().IsDir() {
			panic(fmt.Sprintf("'%s' is not a directory in '%s'", n, i.metadata.Name))
		}

		i = i2.(*inmemoryDirectory)
	}
	return i
}

func (imd *inmemoryDirectory) remove(name string) {
	newChildren := imd.children[:0]

	for _, e := range imd.children {
		if e.Metadata().Name != name {
			newChildren = append(newChildren, e)
		}
	}

	imd.children = newChildren
}

func (imd *inmemoryDirectory) failReaddir(err error) {
	imd.readdirError = err
}

func (imd *inmemoryDirectory) Readdir() (Entries, error) {
	if imd.readdirError != nil {
		return nil, imd.readdirError
	}

	return imd.children, nil
}

type inmemoryFile struct {
	inmemoryEntry

	source     io.Reader
	openError  error
	closeError error
}

type inmemoryFileReader struct {
	io.Reader
	metadata   *EntryMetadata
	closeError error
}

func (ifr *inmemoryFileReader) EntryMetadata() (*EntryMetadata, error) {
	return ifr.metadata, nil
}

func (ifr *inmemoryFileReader) Close() error {
	return ifr.closeError
}

func (imf *inmemoryFile) Open() (EntryMetadataReadCloser, error) {
	if imf.openError != nil {
		return nil, imf.openError
	}
	return &inmemoryFileReader{
		Reader:     imf.source,
		metadata:   imf.metadata,
		closeError: imf.closeError,
	}, nil
}

type inmemorySymlink struct {
	inmemoryEntry
	target string
}

func (imsl *inmemorySymlink) Readlink() (string, error) {
	panic("not implemented yet")
}

func newInMemoryDirectory() *inmemoryDirectory {
	return &inmemoryDirectory{
		inmemoryEntry: inmemoryEntry{
			metadata: &EntryMetadata{
				Name: "<root>",
			},
			parent: nil,
		},
	}
}

var _ Directory = &inmemoryDirectory{}
var _ File = &inmemoryFile{}
var _ Symlink = &inmemorySymlink{}
