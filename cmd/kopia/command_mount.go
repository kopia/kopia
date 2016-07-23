// +build !windows

package main

import (
	"bazil.org/fuse"
	fusefs "bazil.org/fuse/fs"
	"github.com/kopia/kopia/fs"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	mountCommand = app.Command("mount", "Mount repository object as a local filesystem.")

	mountObjectID = mountCommand.Arg("path", "Identifier of the directory to mount.").Required().String()
	mountPoint    = mountCommand.Arg("mountPoint", "Mount point").Required().ExistingDir()
)

type root struct {
	fusefs.Node
}

func (r *root) Root() (fusefs.Node, error) {
	return r.Node, nil
}

func runMountCommand(context *kingpin.ParseContext) error {
	vlt := mustOpenVault()

	r, err := vlt.OpenRepository()
	if err != nil {
		return err
	}

	fuseConnection, err := fuse.Mount(
		*mountPoint,
		fuse.ReadOnly(),
		fuse.FSName("kopia"),
		fuse.Subtype("kopia"),
		fuse.VolumeName("Kopia"),
	)

	oid, err := parseObjectID(*mountObjectID, vlt)
	if err != nil {
		return err
	}

	dir := fs.NewRootDirectoryFromRepository(r, oid)

	rootNode, err := fs.NewFuseDirectory(fs.Logging(dir).(fs.Directory))
	if err != nil {
		return err
	}

	fusefs.Serve(fuseConnection, &root{rootNode})

	return nil
}

func init() {
	mountCommand.Action(runMountCommand)
}
