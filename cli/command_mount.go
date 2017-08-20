// +build !windows

package cli

import (
	"bazil.org/fuse"
	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/loggingfs"
	"github.com/kopia/kopia/fs/repofs"

	fusefs "bazil.org/fuse/fs"
	kopiafuse "github.com/kopia/kopia/fuse"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	mountCommand = app.Command("mount", "Mount repository object as a local filesystem.")

	mountObjectID             = mountCommand.Arg("path", "Identifier of the directory to mount.").Required().String()
	mountPoint                = mountCommand.Arg("mountPoint", "Mount point").Required().ExistingDir()
	mountTraceFS              = mountCommand.Flag("trace-fs", "Trace filesystem operations").Bool()
	mountMaxCachedEntries     = mountCommand.Flag("max-cached-entries", "Limit the number of cached directory entries").Default("100000").Int()
	mountMaxCachedDirectories = mountCommand.Flag("max-cached-dirs", "Limit the number of cached directories").Default("100").Int()
)

type root struct {
	fusefs.Node
}

func (r *root) Root() (fusefs.Node, error) {
	return r.Node, nil
}

func runMountCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)

	fuseConnection, err := fuse.Mount(
		*mountPoint,
		fuse.ReadOnly(),
		fuse.FSName("kopia"),
		fuse.Subtype("kopia"),
		fuse.VolumeName("Kopia"),
	)

	if err != nil {
		return err
	}

	var entry fs.Directory

	if *mountObjectID == "all" {
		entry = repofs.AllSources(rep)
	} else {
		oid, err := parseObjectID(*mountObjectID, rep)
		if err != nil {
			return err
		}
		entry = repofs.Directory(rep, oid)
	}

	if *mountTraceFS {
		entry = loggingfs.Wrap(entry).(fs.Directory)
	}

	cache := kopiafuse.NewCache(
		kopiafuse.MaxCachedDirectories(*mountMaxCachedDirectories),
		kopiafuse.MaxCachedDirectoryEntries(*mountMaxCachedEntries),
	)
	rootNode := kopiafuse.NewDirectoryNode(entry, cache)

	fusefs.Serve(fuseConnection, &root{rootNode})

	return nil
}

func init() {
	mountCommand.Action(runMountCommand)
}
