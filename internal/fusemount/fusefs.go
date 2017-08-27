// +build !windows

// Package fusemount implements FUSE filesystem nodes for mounting contents of filesystem stored in repository.
//
// The FUSE implementation used is from bazil.org/fuse
package fusemount

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/fscache"

	"bazil.org/fuse"
	fusefs "bazil.org/fuse/fs"

	"golang.org/x/net/context"
)

type fuseNode struct {
	entry fs.Entry
	cache *fscache.Cache
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
	reader, err := f.entry.(fs.File).Open()
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}

type fuseDirectoryNode struct {
	fuseNode
	cacheID int64
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

	return newFuseNode(e, dir.cache)
}

func (dir *fuseDirectoryNode) readPossiblyCachedReaddir() (fs.Entries, error) {
	return dir.cache.GetEntries(dir.cacheID, func() (fs.Entries, error) {
		entries, err := dir.directory().Readdir()
		if err != nil {
			return nil, err
		}

		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Metadata().Name < entries[j].Metadata().Name
		})

		return entries, nil
	})
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
	return sl.entry.(fs.Symlink).Readlink()
}

func newFuseNode(e fs.Entry, cache *fscache.Cache) (fusefs.Node, error) {
	switch e := e.(type) {
	case fs.Directory:
		return newDirectoryNode(e, cache), nil
	case fs.File:
		return &fuseFileNode{fuseNode{e, cache}}, nil
	case fs.Symlink:
		return &fuseSymlinkNode{fuseNode{e, cache}}, nil
	default:
		return nil, fmt.Errorf("entry type not supported: %v", e.Metadata().Type)
	}
}

func newDirectoryNode(dir fs.Directory, cache *fscache.Cache) fusefs.Node {
	return &fuseDirectoryNode{fuseNode{dir, cache}, cache.AllocateID()}
}

// NewDirectoryNode returns FUSE Node for a given fs.Directory
func NewDirectoryNode(dir fs.Directory, cache *fscache.Cache) fusefs.Node {
	return newDirectoryNode(dir, cache)
}
