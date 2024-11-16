//go:build !windows && !openbsd && !freebsd
// +build !windows,!openbsd,!freebsd

// Package fusemount implements FUSE filesystem nodes for mounting contents of filesystem stored in repository.
//
// The FUSE implementation used is from github.com/hanwen/go-fuse/v2
package fusemount

import (
	"io"
	"os"
	"sync"
	"syscall"

	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.Module("fuse")

const fakeBlockSize = 4096

type fuseNode struct {
	gofusefs.Inode
	entry fs.Entry
}

func goModeToUnixMode(mode os.FileMode) uint32 {
	unixmode := uint32(mode.Perm())

	if mode&os.ModeSetuid != 0 {
		unixmode |= 0o4000
	}

	if mode&os.ModeSetgid != 0 {
		unixmode |= 0o2000
	}

	if mode&os.ModeSticky != 0 {
		unixmode |= 0o1000
	}

	return unixmode
}

func populateAttributes(a *fuse.Attr, e fs.Entry) {
	a.Mode = goModeToUnixMode(e.Mode())
	a.Size = uint64(e.Size())            //nolint:gosec
	a.Mtime = uint64(e.ModTime().Unix()) //nolint:gosec
	a.Ctime = a.Mtime
	a.Atime = a.Mtime
	a.Nlink = 1
	a.Uid = e.Owner().UserID
	a.Gid = e.Owner().GroupID
	a.Blocks = (a.Size + fakeBlockSize - 1) / fakeBlockSize
}

func (n *fuseNode) Getattr(ctx context.Context, _ gofusefs.FileHandle, a *fuse.AttrOut) syscall.Errno {
	populateAttributes(&a.Attr, n.entry)

	a.Ino = n.StableAttr().Ino

	return gofusefs.OK
}

type fuseFileNode struct {
	fuseNode
}

func (f *fuseFileNode) Open(ctx context.Context, _ uint32) (gofusefs.FileHandle, uint32, syscall.Errno) {
	reader, err := f.entry.(fs.File).Open(ctx)
	if err != nil {
		log(ctx).Errorf("error opening %v: %v", f.entry.Name(), err)

		return nil, 0, syscall.EIO
	}

	return &fuseFileHandle{reader: reader, file: f.entry.(fs.File)}, 0, gofusefs.OK //nolint:forcetypeassert
}

type fuseFileHandle struct {
	mu sync.Mutex

	// +checklocks:mu
	reader fs.Reader

	// +checklocks:mu
	file fs.File
}

func (f *fuseFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, err := f.reader.Seek(off, io.SeekStart)
	if err != nil {
		log(ctx).Errorf("seek error: %v %v: %v", f.file.Name(), off, err)

		return nil, syscall.EIO
	}

	n, err := f.reader.Read(dest)

	if err != nil && !errors.Is(err, io.EOF) {
		log(ctx).Errorf("read error: %v: %v", f.file.Name(), err)
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(dest[0:n]), gofusefs.OK
}

func (f *fuseFileHandle) Release(ctx context.Context) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.reader.Close() //nolint:errcheck

	return gofusefs.OK
}

type fuseDirectoryNode struct {
	fuseNode
}

func (dir *fuseDirectoryNode) directory() fs.Directory {
	return dir.entry.(fs.Directory) //nolint:forcetypeassert
}

func (dir *fuseDirectoryNode) Lookup(ctx context.Context, fileName string, out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {
	e, err := dir.directory().Child(ctx, fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, syscall.ENOENT
		}

		log(ctx).Errorf("lookup error %v in %v: %v", fileName, dir.entry.Name(), err)

		return nil, syscall.EIO
	}

	if e == nil {
		return nil, syscall.ENOENT
	}

	stable := gofusefs.StableAttr{
		Mode: entryToFuseMode(e),
	}

	n, err := newFuseNode(e)
	if err != nil {
		return nil, syscall.EIO
	}

	child := dir.NewInode(ctx, n, stable)

	populateAttributes(&out.Attr, e)

	return child, gofusefs.OK
}

func (dir *fuseDirectoryNode) Readdir(ctx context.Context) (gofusefs.DirStream, syscall.Errno) {
	// TODO: Slice not required as DirStream is also an iterator.
	result := []fuse.DirEntry{}

	iter, err := dir.directory().Iterate(ctx)
	if err != nil {
		log(ctx).Errorf("error reading directory %v: %v", dir.entry.Name(), err)
		return nil, syscall.EIO
	}

	defer iter.Close()

	cur, err := iter.Next(ctx)
	for cur != nil {
		result = append(result, fuse.DirEntry{
			Name: cur.Name(),
			Mode: entryToFuseMode(cur),
		})

		cur, err = iter.Next(ctx)
	}

	if err != nil {
		log(ctx).Errorf("error reading directory %v: %v", dir.entry.Name(), err)
		return nil, syscall.EIO
	}

	return gofusefs.NewListDirStream(result), gofusefs.OK
}

type fuseSymlinkNode struct {
	fuseNode
}

func (sl *fuseSymlinkNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	v, err := sl.entry.(fs.Symlink).Readlink(ctx)
	if err != nil {
		log(ctx).Errorf("error reading symlink %v: %v", sl.entry.Name(), err)
		return nil, syscall.EIO
	}

	return []byte(v), gofusefs.OK
}

func entryToFuseMode(e fs.Entry) uint32 {
	switch e.(type) {
	case fs.File:
		return fuse.S_IFREG
	case fs.Directory:
		return fuse.S_IFDIR
	case fs.Symlink:
		return fuse.S_IFLNK
	default:
		return fuse.S_IFREG
	}
}

func newFuseNode(e fs.Entry) (gofusefs.InodeEmbedder, error) {
	switch e := e.(type) {
	case fs.Directory:
		return newDirectoryNode(e), nil
	case fs.File:
		return &fuseFileNode{fuseNode{entry: e}}, nil
	case fs.Symlink:
		return &fuseSymlinkNode{fuseNode{entry: e}}, nil
	default:
		return nil, errors.Errorf("entry type not supported: %v", e.Mode())
	}
}

func newDirectoryNode(dir fs.Directory) gofusefs.InodeEmbedder {
	return &fuseDirectoryNode{fuseNode{entry: dir}}
}

// NewDirectoryNode returns FUSE Node for a given fs.Directory.
func NewDirectoryNode(dir fs.Directory) gofusefs.InodeEmbedder {
	return newDirectoryNode(dir)
}

var (
	_ gofusefs.NodeGetattrer  = (*fuseNode)(nil)
	_ gofusefs.NodeOpener     = (*fuseFileNode)(nil)
	_ gofusefs.NodeLookuper   = (*fuseDirectoryNode)(nil)
	_ gofusefs.NodeReaddirer  = (*fuseDirectoryNode)(nil)
	_ gofusefs.NodeReadlinker = (*fuseSymlinkNode)(nil)
	_ gofusefs.FileReleaser   = (*fuseFileHandle)(nil)
	_ gofusefs.FileReader     = (*fuseFileHandle)(nil)
)
