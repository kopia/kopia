package cli

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

const(
	snapshotDeleteHelp = `Manually delete a snapshot by providing a snapshot ID.

The snapshot ID is derived from the contents of the snapshot, so identical snapshots
will alwaysh have the same ID.
The manifest ID can also be used, which is always unique to each snapshot even identacal ones.
Get the manifest ID with the ` + "`" + `-m` + "`" + ` flag on for kopia snapshot list

` + "```" + `

$ kopia snapshot delete 3fc72d7eb3a942c28f03196eee0239f5 --delete
	Would delete snapshot 3fc72d7eb3a942c28f03196eee0239f5 of user@host:/home/user at 2023-06-06 10:00:00 UTC (pass --delete to confirm)

` + "```" + `

Deletion must be confirmed wit the --delete flag.
Space used up by unique content in the deleted snapshot is not freed up immediately on the underlying storage.
This is for concurreny safety reasons.
It may take up to 3 days before this space is actually freed up
on the underlying storage. See help for kopia maintenance for more information.

It is also possible to delete all snapshots from a source using ` + "`" + `--all-snapshots-for-source` + "`" + `
This takes the given ` + "`" + `user@host:/path` + "`" + ` or ` + "`" + `user@host` + "`" + ` and deletes all snapshots from that source.

` + "```" + `

$ kopia snapshot delete --all-snapshots-for-source user@host:/path
	Would delete snapshot 37c666eb0d5a63825d9d69ac19ce1d58 of user@host:/path at 2023-05-30 22:00:10 UTC (pass --delete to confirm)
	Would delete snapshot 906040f0eec3f9264dc8e59b94943455 of user@host:/path at 2023-06-02 00:00:10 UTC (pass --delete to confirm)
	Would delete snapshot 1187f14b1c5c6bd1bc24f90644b4993f of user@host:/path at 2023-06-03 09:00:19 UTC (pass --delete to confirm)
	Would delete snapshot 355fdc532a3b13fc7c2e5ecf40992afd of user@host:/path at 2023-06-03 23:00:05 UTC (pass --delete to confirm)

` + "```" + `

>This can be useful for removing an unused source that is no longer needed in the backup. Use with caution as it WILL DELETE ALL SNAPSHOTS
for the given host. This is irreversable. Use ` + "`" + `--no-delete` + "`" + ` for a dry run.
`
)

type commandSnapshotDelete struct {
	snapshotDeleteIDs                   []string
	snapshotDeleteConfirm               bool
	snapshotDeleteAllSnapshotsForSource bool
}

func (c *commandSnapshotDelete) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("delete", snapshotDeleteHelp)
	cmd.Arg("id", "Snapshot ID or root object ID to be deleted").Required().StringsVar(&c.snapshotDeleteIDs)
	cmd.Flag("all-snapshots-for-source", "Delete all snapshots for a source").BoolVar(&c.snapshotDeleteAllSnapshotsForSource)
	cmd.Flag("delete", "Confirm deletion").BoolVar(&c.snapshotDeleteConfirm)
	// hidden flag for backwards compatibility
	cmd.Flag("unsafe-ignore-source", "Alias for --delete").Hidden().BoolVar(&c.snapshotDeleteConfirm)
	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandSnapshotDelete) run(ctx context.Context, rep repo.RepositoryWriter) error {
	if c.snapshotDeleteAllSnapshotsForSource {
		return c.snapshotDeleteSources(ctx, rep)
	}

	for _, id := range c.snapshotDeleteIDs {
		m, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(id))
		if err == nil {
			// snapshot found by manifest ID, delete it directly.
			if err = c.deleteSnapshot(ctx, rep, m); err != nil {
				return errors.Wrapf(err, "error deleting %v", id)
			}
		} else if !errors.Is(err, snapshot.ErrSnapshotNotFound) {
			return errors.Wrapf(err, "error loading snapshot %v", id)
		} else if err := c.deleteSnapshotsByRootObjectID(ctx, rep, id); err != nil {
			return errors.Wrapf(err, "error deleting snapshots by root ID %v", id)
		}
	}

	return nil
}

func (c *commandSnapshotDelete) snapshotDeleteSources(ctx context.Context, rep repo.RepositoryWriter) error {
	for _, source := range c.snapshotDeleteIDs {
		si, err := snapshot.ParseSourceInfo(source, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
		if err != nil {
			return errors.Wrapf(err, "invalid source '%s'", source)
		}

		manifestIDs, err := snapshot.ListSnapshotManifests(ctx, rep, &si, nil)
		if err != nil {
			return errors.Wrapf(err, "error listing manifests for %v", si)
		}

		manifests, err := snapshot.LoadSnapshots(ctx, rep, manifestIDs)
		if err != nil {
			return errors.Wrapf(err, "error loading manifests for %v", si)
		}

		if len(manifests) == 0 {
			return errors.Errorf("no snapshots for source %v", si)
		}

		for _, m := range manifests {
			if err := c.deleteSnapshot(ctx, rep, m); err != nil {
				return errors.Wrap(err, "error deleting")
			}
		}
	}

	return nil
}

func (c *commandSnapshotDelete) deleteSnapshot(ctx context.Context, rep repo.RepositoryWriter, m *snapshot.Manifest) error {
	desc := fmt.Sprintf("snapshot %v of %v at %v", m.ID, m.Source, formatTimestamp(m.StartTime.ToTime()))

	if !c.snapshotDeleteConfirm {
		log(ctx).Infof("Would delete %v (pass --delete to confirm)", desc)
		return nil
	}

	log(ctx).Infof("Deleting %v...", desc)

	return errors.Wrap(rep.DeleteManifest(ctx, m.ID), "error deleting manifest")
}

func (c *commandSnapshotDelete) deleteSnapshotsByRootObjectID(ctx context.Context, rep repo.RepositoryWriter, rootID string) error {
	rootOID, err := object.ParseID(rootID)
	if err != nil {
		return errors.Wrapf(err, "invalid object ID")
	}

	manifests, err := snapshot.FindSnapshotsByRootObjectID(ctx, rep, rootOID)
	if err != nil {
		return errors.Wrapf(err, "unable to find snapshots by root %v", rootID)
	}

	if len(manifests) == 0 {
		return errors.Errorf("no snapshots matched %v", rootID)
	}

	for _, m := range manifests {
		if err := c.deleteSnapshot(ctx, rep, m); err != nil {
			return errors.Wrap(err, "error deleting")
		}
	}

	return nil
}
