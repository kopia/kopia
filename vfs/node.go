package vfs

import (
	fusefs "bazil.org/fuse"
	"github.com/kopia/kopia/fs"
	"golang.org/x/net/context"
)

type node struct {
	manager *manager
	*fs.Entry
}

func (n *node) Attr(ctx context.Context, a *fusefs.Attr) error {
	a.Mode = n.FileMode
	a.Size = uint64(n.FileSize)
	a.Mtime = n.ModTime
	a.Uid = n.OwnerID
	a.Gid = n.GroupID
	return nil
}
