package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/policy"
)

type commandPolicyDelete struct {
	targets []string
	global  bool
	dryRun  bool
}

func (c *commandPolicyDelete) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("delete", "Remove snapshot policy for a single directory, user@host or a global policy.").Alias("remove").Alias("rm")
	cmd.Arg("target", "Target of a policy ('global','user@host','@host') or a path").StringsVar(&c.targets)
	cmd.Flag("global", "Set global policy").BoolVar(&c.global)
	cmd.Flag("dry-run", "Do not remove").Short('n').BoolVar(&c.dryRun)
	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandPolicyDelete) run(ctx context.Context, rep repo.RepositoryWriter) error {
	targets, err := policyTargets(ctx, rep, c.global, c.targets)
	if err != nil {
		return err
	}

	for _, target := range targets {
		log(ctx).Infof("Removing policy on %q...", target)

		if c.dryRun {
			continue
		}

		if err := policy.RemovePolicy(ctx, rep, target); err != nil {
			return errors.Wrapf(err, "error removing policy on %v", target)
		}
	}

	return nil
}
