// +build !windows

package cli

import (
	"context"

	"bazil.org/fuse"
	fusefs "bazil.org/fuse/fs"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/fusemount"
)

type root struct {
	fusefs.Node
}

func (r *root) Root() (fusefs.Node, error) {
	return r.Node, nil
}

var mountMode = mountCommand.Flag("mode", "Mount mode").Default("FUSE").Enum("WEBDAV", "FUSE")

func mountDirectoryFUSE(ctx context.Context, entry fs.Directory, mountPoint string) error {
	rootNode := fusemount.NewDirectoryNode(entry)

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
	defer fuseConnection.Close() //nolint:errcheck

	onCtrlC(func() {
		if unmounterr := fuse.Unmount(mountPoint); unmounterr != nil {
			log(ctx).Warningf("unmount failed: %v", unmounterr)
		}
	})

	err = fusefs.Serve(fuseConnection, &root{rootNode})
	if err != nil {
		return err
	}
	// wait for mount to stop.
	<-fuseConnection.Ready

	return fuseConnection.MountError
}
