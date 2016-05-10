package main

import (
	"os"

	"bazil.org/fuse"
	fusefs "bazil.org/fuse/fs"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/vfs"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	mountCommand = app.Command("mount", "Mount repository object as a local filesystem.")

	mountObjectID = mountCommand.Arg("objectID", "Directory to mount").Required().String()
	mountPoint    = mountCommand.Arg("mountPoint", "Mount point").Required().ExistingDir()
)

type root struct {
	fusefs.Node
}

func (r *root) Root() (fusefs.Node, error) {
	return r.Node, nil
}

func runMountCommand(context *kingpin.ParseContext) error {
	r, err := mustOpenVault().OpenRepository()
	if err != nil {
		return err
	}

	mgr := vfs.NewManager(r)

	fuseConnection, err := fuse.Mount(
		*mountPoint,
		fuse.ReadOnly(),
		fuse.FSName("kopia"),
		fuse.Subtype("kopia"),
		fuse.VolumeName("Kopia"),
	)

	fusefs.Serve(fuseConnection, &root{
		mgr.NewNodeFromEntry(&fs.Entry{
			Name:     "<root>",
			FileMode: os.ModeDir | 0555,
			ObjectID: repo.ObjectID(*mountObjectID),
		}),
	})

	return nil
}

func init() {
	mountCommand.Action(runMountCommand)
}
