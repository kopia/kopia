package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

type commandSnapshotPin struct {
	addPins     []string
	removePins  []string
	snapshotIDs []string
}

func (c *commandSnapshotPin) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("pin", "Add or remove pins preventing snapshot deletion")
	cmd.Flag("add", "Add pins").StringsVar(&c.addPins)
	cmd.Flag("remove", "Remove pins").StringsVar(&c.removePins)
	cmd.Arg("id", "Snapshot ID or root object ID").Required().StringsVar(&c.snapshotIDs)
	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandSnapshotPin) run(ctx context.Context, rep repo.RepositoryWriter) error {
	if len(c.addPins)+len(c.removePins) == 0 {
		return errors.New("must specify --add and/or --remove")
	}

	for _, id := range c.snapshotIDs {
		m, err := snapshot.LoadSnapshot(ctx, rep, manifest.ID(id))
		if err == nil {
			if err = c.pinSnapshot(ctx, rep, m); err != nil {
				return errors.Wrapf(err, "error pinning %v", id)
			}
		} else if !errors.Is(err, snapshot.ErrSnapshotNotFound) {
			return errors.Wrapf(err, "error loading snapshot %v", id)
		} else if err := c.pinSnapshotsByRootObjectID(ctx, rep, id); err != nil {
			return errors.Wrapf(err, "error pinning snapshots by root ID %v", id)
		}
	}

	return nil
}

func (c *commandSnapshotPin) pinSnapshotsByRootObjectID(ctx context.Context, rep repo.RepositoryWriter, rootID string) error {
	rootOID, err := object.ParseID(rootID)
	if err != nil {
		return errors.Wrap(err, "unable to parse object ID")
	}

	manifests, err := snapshot.FindSnapshotsByRootObjectID(ctx, rep, rootOID)
	if err != nil {
		return errors.Wrapf(err, "unable to find snapshots by root %v", rootID)
	}

	if len(manifests) == 0 {
		return errors.Errorf("no snapshots matched %v", rootID)
	}

	for _, m := range manifests {
		if err := c.pinSnapshot(ctx, rep, m); err != nil {
			return errors.Wrap(err, "error pinning")
		}
	}

	return nil
}

func (c *commandSnapshotPin) pinSnapshot(ctx context.Context, rep repo.RepositoryWriter, m *snapshot.Manifest) error {
	if !m.UpdatePins(c.addPins, c.removePins) {
		log(ctx).Infof("No change for snapshot at %v of %v", formatTimestamp(m.StartTime.ToTime()), m.Source)

		return nil
	}

	log(ctx).Infof("Updating snapshot at %v of %v", formatTimestamp(m.StartTime.ToTime()), m.Source)

	return errors.Wrap(snapshot.UpdateSnapshot(ctx, rep, m), "error updating snapshot")
}
