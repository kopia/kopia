package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/pkg/errors"
)

var (
	snapshotDeleteCommand      = snapshotCommands.Command("delete", "Explicitly delete a snapshot by providing a snapshot ID.")
	snapshotDeleteID           = snapshotDeleteCommand.Arg("id", "Snapshot ID to be deleted").Required().String()
	snapshotDeletePath         = snapshotDeleteCommand.Flag("path", "Specify the path of the snapshot to be deleted").String()
	snapshotDeleteIgnoreSource = snapshotDeleteCommand.Flag("unsafe-ignore-source", "Override the requirement to specify source info for the delete to succeed").Bool()
)

func runDeleteCommand(ctx context.Context, rep *repo.Repository) error {
	if !*snapshotDeleteIgnoreSource && *snapshotDeletePath == "" {
		return errors.New("path is required")
	}

	manifestID := manifest.ID(*snapshotDeleteID)
	manifestMeta, err := rep.Manifests.GetMetadata(ctx, manifestID)
	if err != nil {
		return err
	}
	labels := manifestMeta.Labels
	if labels["type"] != "snapshot" {
		return errors.Errorf("snapshot ID provided (%v) did not reference a snapshot", manifestID)
	}
	if !*snapshotDeleteIgnoreSource {
		if labels["hostname"] != getHostName() {
			return errors.New("host name does not match for deleting requested snapshot ID")
		}
		if labels["username"] != getUserName() {
			return errors.New("user name does not match for deleting requested snapshot ID")
		}
		if labels["path"] != *snapshotDeletePath {
			return errors.New("path does not match for deleting requested snapshot ID")
		}
	}

	return rep.Manifests.Delete(ctx, manifestID)
}

func init() {
	addUserAndHostFlags(snapshotDeleteCommand)
	snapshotDeleteCommand.Action(repositoryAction(runDeleteCommand))
}
