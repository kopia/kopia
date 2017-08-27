// +build !windows

package cli

import (
	"bazil.org/fuse"
	fusefs "bazil.org/fuse/fs"
	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/fscache"
	"github.com/kopia/kopia/internal/fusemount"
)

type root struct {
	fusefs.Node
}

func (r *root) Root() (fusefs.Node, error) {
	return r.Node, nil
}

var (
	mountMode = mountCommand.Flag("mode", "Mount mode").Default("FUSE").Enum("WEBDAV", "FUSE")
)

func mountDirectoryFUSE(entry fs.Directory, mountPoint string, cache *fscache.Cache) error {
	rootNode := fusemount.NewDirectoryNode(entry, cache)

	fuseConnection, err := fuse.Mount(
		mountPoint,
		fuse.ReadOnly(),
		fuse.FSName("kopia"),
		fuse.Subtype("kopia"),
		fuse.VolumeName("Kopia"),
	)

	if err != nil {
		return err
	}

	return fusefs.Serve(fuseConnection, &root{rootNode})
}
