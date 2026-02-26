package cli

import (
	"context"
	"fmt"
	"strings"
	"syscall"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/storagereserve"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
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
	// Snapshot deletion is a recovery action.
	// We ensure reserve or delete it if we are already out of space.
	// This only applies to direct repositories.
	if dr, ok := rep.(repo.DirectRepositoryWriter); ok {
		if err := ensureReserveOrDeleteForRecovery(ctx, dr.BlobStorage()); err != nil {
			return err
		}
	}

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

func ensureReserveOrDeleteForRecovery(ctx context.Context, st blob.Storage) error {
	err := storagereserve.Ensure(ctx, st, storagereserve.DefaultReserveSize)
	if err == nil {
		return nil
	}

	// Only attempt sacrificial deletion if we are actually out of space.
	// For other errors (e.g. permissions), we should fail fast.
	if errors.Is(err, storagereserve.ErrInsufficientSpace) || isNoSpaceError(err) {
		log(ctx).Warnf("Could not ensure storage reserve, attempting to delete it to free up space: %v", err)
		if delErr := storagereserve.Delete(ctx, st); delErr != nil {
			return errors.Wrapf(delErr, "emergency cleanup failed after original error: %v", err)
		}

		return nil
	}

	return errors.Wrap(err, "error ensuring storage reserve")
}

// isNoSpaceError is a helper to detect out-of-space conditions consistently.
// This matches the implementation in repo/maintenance/maintenance_run.go.
func isNoSpaceError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, syscall.ENOSPC) {
		return true
	}

	msg := strings.ToLower(err.Error())
	noSpacePhrases := []string{
		"no space left on device",
		"no space left on disk",
		"not enough space",
		"insufficient space",
		"disk is full",
	}

	for _, phrase := range noSpacePhrases {
		if strings.Contains(msg, phrase) {
			return true
		}
	}

	return false
}
