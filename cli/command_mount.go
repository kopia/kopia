package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/cachefs"
	"github.com/kopia/kopia/fs/loggingfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var (
	mountCommand = app.Command("mount", "Mount repository object as a local filesystem.")

	mountObjectID = mountCommand.Arg("path", "Identifier of the directory to mount.").Required().String()
	mountPoint    = mountCommand.Arg("mountPoint", "Mount point").Required().String()
	mountTraceFS  = mountCommand.Flag("trace-fs", "Trace filesystem operations").Bool()
)

func runMountCommand(ctx context.Context, rep repo.Repository) error {
	var entry fs.Directory

	if *mountObjectID == "all" {
		entry = snapshotfs.AllSourcesEntry(rep)
	} else {
		oid, err := parseObjectID(ctx, rep, *mountObjectID)
		if err != nil {
			return err
		}
		entry = snapshotfs.DirectoryEntry(rep, oid, nil)
	}

	if *mountTraceFS {
		entry = loggingfs.Wrap(entry, log(ctx).Debugf).(fs.Directory)
	}

	entry = cachefs.Wrap(entry, newFSCache()).(fs.Directory)

	switch *mountMode {
	case "FUSE":
		return mountDirectoryFUSE(ctx, entry, *mountPoint)
	case "WEBDAV":
		return mountDirectoryWebDAV(ctx, entry, *mountPoint)
	default:
		return errors.Errorf("unsupported mode: %q", *mountMode)
	}
}

func init() {
	setupFSCacheFlags(mountCommand)
	mountCommand.Action(repositoryAction(runMountCommand))
}
