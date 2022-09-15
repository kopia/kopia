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

type commandSnapshotDelete struct {
	snapshotDeleteIDs                   []string
	snapshotDeleteConfirm               bool
	snapshotDeleteAllSnapshotsForSource bool
}

func (c *commandSnapshotDelete) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("delete", "Explicitly delete a snapshot by providing a snapshot ID.")
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
