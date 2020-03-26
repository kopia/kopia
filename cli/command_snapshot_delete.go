package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
)

var (
	snapshotDeleteCommand      = snapshotCommands.Command("delete", "Explicitly delete a snapshot by providing a snapshot ID.")
	snapshotDeleteID           = snapshotDeleteCommand.Arg("id", "Snapshot ID to be deleted").Required().String()
	snapshotDeletePath         = snapshotDeleteCommand.Flag("path", "Specify the path of the snapshot to be deleted").String()
	snapshotDeleteHostname     = snapshotDeleteCommand.Flag("hostname", "Specify the hostname of the snapshot to be deleted").String()
	snapshotDeleteUsername     = snapshotDeleteCommand.Flag("username", "Specify the username of the snapshot to be deleted").String()
	snapshotDeleteIgnoreSource = snapshotDeleteCommand.Flag("unsafe-ignore-source", "Override the requirement to specify source info for the delete to succeed").Bool()
)

func runDeleteCommand(ctx context.Context, rep repo.Repository) error {
	if !*snapshotDeleteIgnoreSource && *snapshotDeletePath == "" {
		return errors.New("path is required")
	}

	manifestID := manifest.ID(*snapshotDeleteID)

	manifestMeta, err := rep.GetManifest(ctx, manifestID, nil)
	if err != nil {
		return err
	}

	labels := manifestMeta.Labels
	if labels["type"] != "snapshot" {
		return errors.Errorf("snapshot ID provided (%v) did not reference a snapshot", manifestID)
	}

	if !*snapshotDeleteIgnoreSource {
		h := *snapshotDeleteHostname
		if h == "" {
			h = rep.Hostname()
		}

		if labels["hostname"] != h {
			return errors.Errorf("host name does not match for deleting requested snapshot ID (got %q, expected %q)", h, labels["hostname"])
		}

		u := *snapshotDeleteUsername
		if u == "" {
			u = rep.Username()
		}

		if labels["username"] != u {
			return errors.Errorf("user name does not match for deleting requested snapshot ID (got %q, expected %q)", u, labels["username"])
		}

		if labels["path"] != *snapshotDeletePath {
			return errors.New("path does not match for deleting requested snapshot ID")
		}
	}

	return rep.DeleteManifest(ctx, manifestID)
}

func init() {
	snapshotDeleteCommand.Action(repositoryAction(runDeleteCommand))
}
