package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/acl"
	"github.com/kopia/kopia/internal/auth"
	"github.com/kopia/kopia/repo"
)

type commandACLEnable struct {
	reset bool
}

func (c *commandACLEnable) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("enable", "Enable ACLs and install default entries")
	cmd.Flag("reset", "Reset all ACLs to default").BoolVar(&c.reset)
	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandACLEnable) run(ctx context.Context, rep repo.RepositoryWriter) error {
	entries, err := acl.LoadEntries(ctx, rep, nil)
	if err != nil {
		return errors.Wrap(err, "error loading ACL entries")
	}

	if len(entries) != 0 && !c.reset {
		return errors.New("ACLs already enabled")
	}

	if c.reset {
		for _, e := range entries {
			log(ctx).Infof("deleting previous ACL entry %v", e.ManifestID)

			if err := rep.DeleteManifest(ctx, e.ManifestID); err != nil {
				return errors.Wrap(err, "unable to delete previous ACL")
			}
		}
	}

	for _, e := range auth.DefaultACLs {
		if err := acl.AddACL(ctx, rep, e, false); err != nil {
			return errors.Wrap(err, "unable to add default ACL")
		}
	}

	return nil
}
