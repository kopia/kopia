package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/acl"
	"github.com/kopia/kopia/repo"
)

type commandACLDelete struct {
	ids     []string
	all     bool
	confirm bool
}

func (c *commandACLDelete) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("delete", "Delete ACL entry").Alias("remove").Alias("rm")
	cmd.Arg("id", "Entry ID").StringsVar(&c.ids)
	cmd.Flag("all", "Remove all ACL entries").BoolVar(&c.all)
	cmd.Flag("delete", "Really delete").BoolVar(&c.confirm)
	cmd.Action(svc.repositoryWriterAction(c.run))
}

func dryRunDelete(ctx context.Context, e *acl.Entry) {
	log(ctx).Infof("would delete entry %v, pass --delete to actually delete", e.ManifestID)
}

func (c *commandACLDelete) shouldRemoveACLEntry(ctx context.Context, e *acl.Entry) bool {
	if c.all {
		if !c.confirm {
			dryRunDelete(ctx, e)
			return false
		}

		return true
	}

	for _, tr := range c.ids {
		if tr == string(e.ManifestID) {
			if !c.confirm {
				dryRunDelete(ctx, e)
				return false
			}

			return true
		}
	}

	return false
}

func (c *commandACLDelete) run(ctx context.Context, rep repo.RepositoryWriter) error {
	entries, err := acl.LoadEntries(ctx, rep, nil)
	if err != nil {
		return errors.Wrap(err, "unable to load entries")
	}

	for _, e := range entries {
		if c.shouldRemoveACLEntry(ctx, e) {
			if err := rep.DeleteManifest(ctx, e.ManifestID); err != nil {
				return errors.Wrap(err, "unable to delete manifest")
			}
		}
	}

	return nil
}
