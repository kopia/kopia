package vfs

import (
	"io"
	"os"

	fusefs "bazil.org/fuse/fs"

	"github.com/kopia/kopia/cas"
	"github.com/kopia/kopia/fs"
)

type Manager interface {
	NewNodeFromEntry(e *fs.Entry) fusefs.Node
}

type manager struct {
	repo  cas.Repository
	cache *dirCache
}

func (mgr *manager) NewNodeFromEntry(e *fs.Entry) fusefs.Node {
	switch e.FileMode & os.ModeType {
	case os.ModeDir:
		return &directoryNode{node{mgr, e}}

	default:
		return &fileNode{node{mgr, e}}
	}
}

func (mgr *manager) readDirectory(oid cas.ObjectID) (fs.Directory, error) {
	if d := mgr.cache.Get(oid); d != nil {
		return d, nil
	}

	r, err := mgr.open(oid)
	if err != nil {
		return nil, err
	}

	d, err := fs.ReadDirectory(r, "")
	if err == nil {
		mgr.cache.Add(oid, d)
	}
	return d, nil
}

func (mgr *manager) open(oid cas.ObjectID) (io.ReadSeeker, error) {
	return mgr.repo.Open(oid)
}

func NewManager(repo cas.Repository) Manager {
	return &manager{
		repo:  repo,
		cache: newDirCache(10000, 1000000),
	}
}
