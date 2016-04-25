package vfs

import (
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

type directoryNode struct {
	node
}

func (dir *directoryNode) Lookup(ctx context.Context, fileName string) (fs.Node, error) {
	entries, err := dir.manager.readDirectory(dir.ObjectID)
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

	return dir.manager.NewNodeFromEntry(e), nil

}

func (dir *directoryNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	entries, err := dir.manager.readDirectory(dir.ObjectID)
	if err != nil {
		return nil, err
	}

	result := []fuse.Dirent{}

	for _, e := range entries {
		dirent := fuse.Dirent{
			Name: e.Name,
		}

		switch e.FileMode & os.ModeType {
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
