package cli

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/cachefs"
	"github.com/kopia/kopia/fs/loggingfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

var (
	mountCommand = app.Command("mount", "Mount repository object as a local filesystem.")

	mountObjectID = mountCommand.Arg("path", "Identifier of the directory to mount.").Required().String()
	mountPoint    = mountCommand.Arg("mountPoint", "Mount point").Required().String()
	mountTraceFS  = mountCommand.Flag("trace-fs", "Trace filesystem operations").Bool()
)

func runMountCommand(ctx context.Context, rep *repo.Repository) error {
	mgr := snapshot.NewManager(rep)
	var entry fs.Directory

	if *mountObjectID == "all" {
		entry = mgr.AllSourcesEntry()
	} else {
		oid, err := parseObjectID(ctx, mgr, *mountObjectID)
		if err != nil {
			return err
		}
		entry = mgr.DirectoryEntry(oid, nil)
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
	mountCommand.Action(repositoryAction(runMountCommand))
}
