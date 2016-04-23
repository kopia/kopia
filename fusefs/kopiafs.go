package fusefs

import (
	"io/ioutil"
	"os"

	kopiafs "github.com/kopia/kopia/fs"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

type fileSystem struct {
	reader kopiafs.Reader
}

// NewFS returns implementation of FUSE filesystem that reads contents of the specified backup.
func NewFS(reader kopiafs.Reader) fs.FS {
	return &fileSystem{reader: reader}
}

func (fs *fileSystem) Root() (fs.Node, error) {
	return &directoryNode{
		fs:       fs,
		fullPath: "/",
	}, nil
}

type directoryNode struct {
	kopiafs.Entry

	fs       *fileSystem
	fullPath string
}

func (dir *directoryNode) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0555
	a.Mtime = dir.ModTime
	a.Uid = dir.OwnerID
	a.Gid = dir.GroupID

	return nil
}

func (dir *directoryNode) Lookup(ctx context.Context, fullPath string) (fs.Node, error) {
	e, err := dir.fs.reader.GetEntry(dir.fullPath + "/" + fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fuse.ENOENT
		}

		return nil, err
	}

	switch e.FileMode & os.ModeType {
	case os.ModeDir:
		return &directoryNode{
			Entry:    e,
			fs:       dir.fs,
			fullPath: dir.fullPath + "/" + e.Name,
		}, nil

	case 0:
		return &fileNode{Entry: e, dir: dir}, nil
	}

	return nil, fuse.ENOENT
}

func (dir *directoryNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	entries, err := dir.fs.reader.ReadDirectory(dir.fullPath)
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

// fileNode implements both Node and Handle.
type fileNode struct {
	kopiafs.Entry

	dir *directoryNode
}

func (f *fileNode) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = f.FileMode & os.ModePerm
	a.Size = uint64(f.FileSize)
	a.Mtime = f.ModTime
	a.Uid = f.OwnerID
	a.Gid = f.GroupID
	return nil
}

func (f *fileNode) ReadAll(ctx context.Context) ([]byte, error) {
	reader, err := f.dir.fs.reader.Open(f.dir.fullPath + "/" + f.Name)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(reader)
}
