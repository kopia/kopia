// +build !windows

// Package fusemount implements FUSE filesystem nodes for mounting contents of filesystem stored in repository.
//
// The FUSE implementation used is from bazil.org/fuse
package fusemount

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/kopia/kopia/fs"

	"bazil.org/fuse"
	fusefs "bazil.org/fuse/fs"

	"golang.org/x/net/context"
)

type fuseNode struct {
	entry fs.Entry
}

func (n *fuseNode) Attr(ctx context.Context, a *fuse.Attr) error {
	m := n.entry.Metadata()
	a.Mode = m.FileMode()
	a.Size = uint64(m.FileSize)
	a.Mtime = m.ModTime
	a.Uid = m.UserID
	a.Gid = m.GroupID
	return nil
}

type fuseFileNode struct {
	fuseNode
}

func (f *fuseFileNode) ReadAll(ctx context.Context) ([]byte, error) {
	reader, err := f.entry.(fs.File).Open(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close() //nolint:errcheck

	return ioutil.ReadAll(reader)
}

type fuseDirectoryNode struct {
	fuseNode
}

func (dir *fuseDirectoryNode) directory() fs.Directory {
	return dir.entry.(fs.Directory)
}

func (dir *fuseDirectoryNode) Lookup(ctx context.Context, fileName string) (fusefs.Node, error) {
	entries, err := dir.directory().Readdir(ctx)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fuse.ENOENT
		}

		return nil, err
	}

	e := entries.FindByName(fileName)
	if e == nil {
		return nil, fuse.ENOENT
	}

	return newFuseNode(e)
}

func (dir *fuseDirectoryNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	entries, err := dir.directory().Readdir(ctx)
	if err != nil {
		return nil, err
	}

	result := []fuse.Dirent{}

	for _, e := range entries {
		m := e.Metadata()
		dirent := fuse.Dirent{
			Name: m.Name,
		}

		switch m.Type {
		case fs.EntryTypeDirectory:
			dirent.Type = fuse.DT_Dir
		case fs.EntryTypeFile:
			dirent.Type = fuse.DT_File
		case fs.EntryTypeSymlink:
			dirent.Type = fuse.DT_Link
		}

		result = append(result, dirent)
	}

	return result, nil
}

type fuseSymlinkNode struct {
	fuseNode
}

func (sl *fuseSymlinkNode) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	return sl.entry.(fs.Symlink).Readlink(ctx)
}

func newFuseNode(e fs.Entry) (fusefs.Node, error) {
	switch e := e.(type) {
	case fs.Directory:
		return newDirectoryNode(e), nil
	case fs.File:
		return &fuseFileNode{fuseNode{e}}, nil
	case fs.Symlink:
		return &fuseSymlinkNode{fuseNode{e}}, nil
	default:
		return nil, fmt.Errorf("entry type not supported: %v", e.Metadata().Type)
	}
}

func newDirectoryNode(dir fs.Directory) fusefs.Node {
	return &fuseDirectoryNode{fuseNode{dir}}
}

// NewDirectoryNode returns FUSE Node for a given fs.Directory
func NewDirectoryNode(dir fs.Directory) fusefs.Node {
	return newDirectoryNode(dir)
}
