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

	mountFuseAllowOther         = mountCommand.Flag("fuse-allow-other", "Allows other users to access the file system.").Bool()
	mountFuseAllowNonEmptyMount = mountCommand.Flag("fuse-allow-non-empty-mount", "Allows the mounting over a non-empty directory. The files in it will be shadowed by the freshly created mount.").Bool()
)

func runMountCommand(ctx context.Context, rep repo.Repository) error {
	var entry fs.Directory

	if *mountObjectID == "all" {
		entry = snapshotfs.AllSourcesEntry(rep)
	} else {
		var err error
		entry, err = snapshotfs.FilesystemDirectoryFromIDWithPath(ctx, rep, *mountObjectID, false)
		if err != nil {
			return errors.Wrapf(err, "unable to get directory entry for %v", *mountObjectID)
		}
	}

	if *mountTraceFS {
		entry = loggingfs.Wrap(entry, log(ctx).Debugf).(fs.Directory)
	}

	entry = cachefs.Wrap(entry, newFSCache()).(fs.Directory)

	ctrl, mountErr := mount.Directory(ctx, entry, *mountPoint,
		mount.Options{
			FuseAllowOther:         *mountFuseAllowOther,
			FuseAllowNonEmptyMount: *mountFuseAllowNonEmptyMount,
		})

	if mountErr != nil {
		return errors.Wrap(mountErr, "mount error")
	}

	log(ctx).Infof("Mounted '%v' on %v", *mountObjectID, ctrl.MountPath())

	if *mountPoint == "*" && !*mountPointBrowse {
		log(ctx).Infof("HINT: Pass --browse to automatically open file browser.")
	}

	log(ctx).Infof("Press Ctrl-C to unmount.")

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
		log(ctx).Infof("Unmounting...")
		// TODO: Consider lazy unmounting (-z) and polling till the filesystem is unmounted instead of failing with:
		// "unmount error: exit status 1: fusermount: failed to unmount /tmp/kopia-mount719819963: Device or resource busy, try --help"
		err := ctrl.Unmount(ctx)
		if err != nil {
			return errors.Wrap(err, "unmount error")
		}

	case <-ctrl.Done():
		log(ctx).Infof("Unmounted.")
		return nil
	}

	// Reporting clean unmount in case of interrupt signal.
	<-ctrl.Done()
	log(ctx).Infof("Unmounted.")

	return nil
}

func init() {
	setupFSCacheFlags(mountCommand)
	mountCommand.Action(repositoryAction(runMountCommand))
}
