package cli

import (
	"context"

	"github.com/pkg/errors"
	"github.com/skratchdot/open-golang/open"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/cachefs"
	"github.com/kopia/kopia/fs/loggingfs"
	"github.com/kopia/kopia/internal/mount"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

var (
	mountCommand = app.Command("mount", "Mount repository object as a local filesystem.")

	mountObjectID    = mountCommand.Arg("path", "Identifier of the directory to mount.").Default("all").String()
	mountPoint       = mountCommand.Arg("mountPoint", "Mount point").Default("*").String()
	mountPointBrowse = mountCommand.Flag("browse", "Open file browser").Bool()
	mountTraceFS     = mountCommand.Flag("trace-fs", "Trace filesystem operations").Bool()
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

	ctrl, mountErr := mount.Directory(ctx, entry, *mountPoint)

	if mountErr != nil {
		return errors.Wrap(mountErr, "mount error")
	}

	printStderr("Mounted '%v' on %v\n", *mountObjectID, ctrl.MountPath())

	if *mountPoint == "*" && !*mountPointBrowse {
		printStderr("HINT: Pass --browse to automatically open file browser.\n")
	}

	printStderr("Press Ctrl-C to unmount.\n")

	if *mountPointBrowse {
		if err := open.Start(ctrl.MountPath()); err != nil {
			log(ctx).Warningf("unable to browse %v", err)
		}
	}

	// Wait until ctrl-c pressed or until the directory is unmounted.
	ctrlCPressed := make(chan bool)

	onCtrlC(func() {
		close(ctrlCPressed)
	})
	select {
	case <-ctrlCPressed:
		printStderr("Unmounting...\n")
		return ctrl.Unmount(ctx)

	case <-ctrl.Done():
		printStderr("Unmounted.\n")
		return nil
	}
}

func init() {
	setupFSCacheFlags(mountCommand)
	mountCommand.Action(repositoryAction(runMountCommand))
}
