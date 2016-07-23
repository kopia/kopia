// +build !windows

package fs

import (
	"fmt"
	"io/ioutil"
	"os"

	"bazil.org/fuse"
	fusefs "bazil.org/fuse/fs"

	"golang.org/x/net/context"
)

type fuseNode struct {
	entry Entry
}

func (n *fuseNode) Attr(ctx context.Context, a *fuse.Attr) error {
	m := n.entry.Metadata()
	a.Mode = m.FileMode
	a.Size = uint64(m.FileSize)
	a.Mtime = m.ModTime
	a.Uid = m.OwnerID
	a.Gid = m.GroupID
	return nil
}

type fuseFileNode struct {
	fuseNode
}

func (f *fuseFileNode) ReadAll(ctx context.Context) ([]byte, error) {
	reader, err := f.entry.(File).Open()
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}

type fuseDirectoryNode struct {
	fuseNode
}

func (dir *fuseDirectoryNode) directory() Directory {
	return dir.entry.(Directory)
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

func (dir *fuseDirectoryNode) readPossiblyCachedReaddir() (Entries, error) {
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

		switch m.FileMode & os.ModeType {
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
	return string(sl.entry.Metadata().ObjectID), nil
}

func newFuseNode(e Entry) (fusefs.Node, error) {
	switch e := e.(type) {
	case Directory:
		return &fuseDirectoryNode{fuseNode{e}}, nil
	case File:
		return &fuseFileNode{fuseNode{e}}, nil
	case Symlink:
		return &fuseSymlinkNode{fuseNode{e}}, nil
	default:
		return nil, fmt.Errorf("entry type not supported: %v", e.Metadata().FileMode)
	}
}

func NewFuseDirectory(e Directory) (fusefs.Node, error) {
	return newFuseNode(e)
}
