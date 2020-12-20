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

var (
	snapshotDeleteCommand = snapshotCommands.Command("delete", "Explicitly delete a snapshot by providing a snapshot ID.")
	snapshotDeleteIDs     = snapshotDeleteCommand.Arg("id", "Snapshot ID or root object ID to be deleted").Required().Strings()
	snapshotDeleteConfirm = snapshotDeleteCommand.Flag("delete", "Confirm deletion").Bool()
)

func runDeleteCommand(ctx context.Context, rep repo.Repository) error {
	for _, id := range *snapshotDeleteIDs {
		m, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(id))
		if err == nil {
			// snapshot found by manifest ID, delete it directly.
			if err = deleteSnapshot(ctx, rep, m); err != nil {
				return errors.Wrapf(err, "error deleting %v", id)
			}
		} else if !errors.Is(err, snapshot.ErrSnapshotNotFound) {
			return errors.Wrapf(err, "error loading snapshot %v", id)
		} else if err := deleteSnapshotsByRootObjectID(ctx, rep, object.ID(id)); err != nil {
			return errors.Wrapf(err, "error deleting snapshots by root ID %v", id)
		}
	}

	return nil
}

func deleteSnapshot(ctx context.Context, rep repo.Repository, m *snapshot.Manifest) error {
	desc := fmt.Sprintf("snapshot %v of %v at %v", m.ID, m.Source, formatTimestamp(m.StartTime))

	if !*snapshotDeleteConfirm {
		log(ctx).Infof("Would delete %v (pass --delete to confirm)\n", desc)
		return nil
	}

	log(ctx).Infof("Deleting %v...", desc)

	return rep.DeleteManifest(ctx, m.ID)
}

func deleteSnapshotsByRootObjectID(ctx context.Context, rep repo.Repository, rootID object.ID) error {
	manifests, err := snapshot.FindSnapshotsByRootObjectID(ctx, rep, rootID)
	if err != nil {
		return errors.Wrapf(err, "unable to find snapshots by root %v", rootID)
	}

	if len(manifests) == 0 {
		return errors.Errorf("no snapshots matched %v", rootID)
	}

	for _, m := range manifests {
		if err := deleteSnapshot(ctx, rep, m); err != nil {
			return errors.Wrap(err, "error deleting")
		}
	}

	return nil
}

func init() {
	snapshotDeleteCommand.Action(repositoryAction(runDeleteCommand))

	// hidden flag for backwards compatibility
	snapshotDeleteCommand.Flag("unsafe-ignore-source", "Alias for --delete").Hidden().BoolVar(snapshotDeleteConfirm)
}
