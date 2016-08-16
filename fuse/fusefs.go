// +build !windows

package fuse

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
	a.Mtime = m.ModTime()
	a.Uid = m.Uid
	a.Gid = m.Gid
	return nil
}

type fuseFileNode struct {
	fuseNode
}

func (f *fuseFileNode) ReadAll(ctx context.Context) ([]byte, error) {
	reader, err := f.entry.(fs.File).Open()
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}

type fuseDirectoryNode struct {
	fuseNode
}

func (dir *fuseDirectoryNode) directory() fs.Directory {
	return dir.entry.(fs.Directory)
}

func (dir *fuseDirectoryNode) Lookup(ctx context.Context, fileName string) (fusefs.Node, error) {
	entries, err := dir.readPossiblyCachedReaddir()
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

func (dir *fuseDirectoryNode) readPossiblyCachedReaddir() (fs.Entries, error) {
	return dir.directory().Readdir()
}

func (dir *fuseDirectoryNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	entries, err := dir.readPossiblyCachedReaddir()
	if err != nil {
		return nil, err
	}

	result := []fuse.Dirent{}

	for _, e := range entries {
		m := e.Metadata()
		dirent := fuse.Dirent{
			Name: m.Name,
		}

		switch m.FileMode() & os.ModeType {
		case os.ModeDir:
			dirent.Type = fuse.DT_Dir
		case 0:
			dirent.Type = fuse.DT_File
		case os.ModeSocket:
			dirent.Type = fuse.DT_Socket
		case os.ModeSymlink:
			dirent.Type = fuse.DT_Link
		case os.ModeNamedPipe:
			dirent.Type = fuse.DT_FIFO
		}

		result = append(result, dirent)
	}

	return result, nil
}

type fuseSymlinkNode struct {
	fuseNode
}

func (sl *fuseSymlinkNode) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	return sl.entry.(fs.Symlink).Readlink()
}

func newFuseNode(e fs.Entry) (fusefs.Node, error) {
	switch e := e.(type) {
	case fs.Directory:
		return &fuseDirectoryNode{fuseNode{e}}, nil
	case fs.File:
		return &fuseFileNode{fuseNode{e}}, nil
	case fs.Symlink:
		return &fuseSymlinkNode{fuseNode{e}}, nil
	default:
		return nil, fmt.Errorf("entry type not supported: %v", e.Metadata().FileMode())
	}
}

// NewDirectoryNode returns fusefs.Node for given fs.Directory
func NewDirectoryNode(dir fs.Directory) fusefs.Node {
	return &fuseDirectoryNode{fuseNode{dir}}
}
