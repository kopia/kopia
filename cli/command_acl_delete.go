package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/acl"
	"github.com/kopia/kopia/repo"
)

var (
	aclRemoveCommand = aclCommands.Command("delete", "Delete ACL entry").Alias("remove").Alias("rm")
	aclRemoveIDs     = aclRemoveCommand.Arg("id", "Entry ID").Strings()
	aclRemoveAll     = aclRemoveCommand.Flag("all", "Remove all ACL entries").Bool()
	aclRemoveConfirm = aclRemoveCommand.Flag("delete", "Really delete").Bool()
)

func dryRunDelete(ctx context.Context, e *acl.Entry) {
	log(ctx).Infof("would delete entry %v, pass --delete to actually delete", e.ManifestID)
}

func shouldRemoveACLEntry(ctx context.Context, e *acl.Entry) bool {
	if *aclRemoveAll {
		if !*aclRemoveConfirm {
			dryRunDelete(ctx, e)
			return false
		}

		return true
	}

	for _, tr := range *aclRemoveIDs {
		if tr == string(e.ManifestID) {
			if !*aclRemoveConfirm {
				dryRunDelete(ctx, e)
				return false
			}

			return true
		}
	}

	return false
}

func runACLRemove(ctx context.Context, rep repo.RepositoryWriter) error {
	entries, err := acl.LoadEntries(ctx, rep, nil)
	if err != nil {
		return errors.Wrap(err, "unable to load entries")
	}

	for _, e := range entries {
		if shouldRemoveACLEntry(ctx, e) {
			if err := rep.DeleteManifest(ctx, e.ManifestID); err != nil {
				return errors.Wrap(err, "unable to delete manifest")
			}
		}
	}

	return nil
}

func init() {
	aclRemoveCommand.Action(repositoryWriterAction(runACLRemove))
}
