package cli

import (
	"fmt"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/cachefs"
	"github.com/kopia/kopia/fs/loggingfs"
	"github.com/kopia/kopia/fs/repofs"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	mountCommand = app.Command("mount", "Mount repository object as a local filesystem.")

	mountObjectID             = mountCommand.Arg("path", "Identifier of the directory to mount.").Required().String()
	mountPoint                = mountCommand.Arg("mountPoint", "Mount point").Required().ExistingDir()
	mountTraceFS              = mountCommand.Flag("trace-fs", "Trace filesystem operations").Bool()
	mountCacheRefreshInterval = mountCommand.Flag("cache-refresh", "Cache refresh interval").Default("600s").Duration()
)

func runMountCommand(context *kingpin.ParseContext) error {
	rep := mustOpenRepository(nil)
	var entry fs.Directory

	if *mountCacheRefreshInterval > 0 {
		go func() {
			for {
				select {
				case <-time.After(*mountCacheRefreshInterval):
					rep.RefreshCache()
					// TODO - cancel the loop perhaps?
				}
			}
		}()
	}

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

	entry = cachefs.Wrap(entry, newFSCache()).(fs.Directory)

	switch *mountMode {
	case "FUSE":
		return mountDirectoryFUSE(entry, *mountPoint)
	case "WEBDAV":
		return mountDirectoryWebDAV(entry, *mountPoint)
	default:
		return fmt.Errorf("unsupported mode: %q", *mountMode)
	}
}

func init() {
	setupFSCacheFlags(mountCommand)
	mountCommand.Action(runMountCommand)
}
