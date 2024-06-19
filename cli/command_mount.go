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

type commandMount struct {
	mountObjectID               string
	mountPoint                  string
	mountPointBrowse            bool
	mountTraceFS                bool
	mountFuseAllowOther         bool
	mountFuseAllowNonEmptyMount bool
	mountPreferWebDAV           bool
	maxCachedEntries            int
	maxCachedDirectories        int

	svc appServices
}

func (c *commandMount) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("mount", "Mount repository object as a local filesystem.")

	cmd.Arg("path", "Identifier of the directory to mount.").Default("all").StringVar(&c.mountObjectID)
	cmd.Arg("mountPoint", "Mount point").Default("*").StringVar(&c.mountPoint)
	cmd.Flag("browse", "Open file browser").BoolVar(&c.mountPointBrowse)
	cmd.Flag("trace-fs", "Trace filesystem operations").BoolVar(&c.mountTraceFS)

	cmd.Flag("fuse-allow-other", "Allows other users to access the file system.").BoolVar(&c.mountFuseAllowOther)
	cmd.Flag("fuse-allow-non-empty-mount", "Allows the mounting over a non-empty directory. The files in it will be shadowed by the freshly created mount.").BoolVar(&c.mountFuseAllowNonEmptyMount)
	cmd.Flag("webdav", "Use WebDAV to mount the repository object regardless of fuse availability.").BoolVar(&c.mountPreferWebDAV)

	cmd.Flag("max-cached-entries", "Limit the number of cached directory entries").Default("100000").IntVar(&c.maxCachedEntries)
	cmd.Flag("max-cached-dirs", "Limit the number of cached directories").Default("100").IntVar(&c.maxCachedDirectories)

	c.svc = svc
	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandMount) newFSCache() cachefs.DirectoryCacher {
	return cachefs.NewCache(&cachefs.Options{
		MaxCachedDirectories: c.maxCachedDirectories,
		MaxCachedEntries:     c.maxCachedEntries,
	})
}

func (c *commandMount) run(ctx context.Context, rep repo.Repository) error {
	var entry fs.Directory

	if c.mountObjectID == "all" {
		entry = snapshotfs.AllSourcesEntry(rep)
	} else {
		var err error

		entry, err = snapshotfs.FilesystemDirectoryFromIDWithPath(ctx, rep, c.mountObjectID, false)
		if err != nil {
			return errors.Wrapf(err, "unable to get directory entry for %v", c.mountObjectID)
		}
	}

	if c.mountTraceFS {
		//nolint:forcetypeassert
		entry = loggingfs.Wrap(entry, log(ctx).Debugf).(fs.Directory)
	}

	//nolint:forcetypeassert
	entry = cachefs.Wrap(entry, c.newFSCache()).(fs.Directory)

	ctrl, mountErr := mount.Directory(ctx, entry, c.mountPoint,
		mount.Options{
			FuseAllowOther:         c.mountFuseAllowOther,
			FuseAllowNonEmptyMount: c.mountFuseAllowNonEmptyMount,
			PreferWebDAV:           c.mountPreferWebDAV,
		})

	if mountErr != nil {
		return errors.Wrap(mountErr, "mount error")
	}

	log(ctx).Infof("Mounted '%v' on %v", c.mountObjectID, ctrl.MountPath())

	if c.mountPoint == "*" && !c.mountPointBrowse {
		log(ctx).Info("HINT: Pass --browse to automatically open file browser.")
	}

	log(ctx).Info("Press Ctrl-C to unmount.")

	if c.mountPointBrowse {
		if err := open.Start(ctrl.MountPath()); err != nil {
			log(ctx).Errorf("unable to browse %v", err)
		}
	}

	// Wait until ctrl-c pressed or until the directory is unmounted.
	ctrlCPressed := make(chan bool)

	c.svc.onTerminate(func() {
		close(ctrlCPressed)
	})

	select {
	case <-ctrlCPressed:
		log(ctx).Info("Unmounting...")
		// TODO: Consider lazy unmounting (-z) and polling till the filesystem is unmounted instead of failing with:
		// "unmount error: exit status 1: fusermount: failed to unmount /tmp/kopia-mount719819963: Device or resource busy, try --help"
		err := ctrl.Unmount(ctx)
		if err != nil {
			return errors.Wrap(err, "unmount error")
		}

	case <-ctrl.Done():
		log(ctx).Info("Unmounted.")
		return nil
	}

	// Reporting clean unmount in case of interrupt signal.
	<-ctrl.Done()
	log(ctx).Info("Unmounted.")

	return nil
}
