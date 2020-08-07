package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/policy"
)

var (
	policyRemoveCommand = policyCommands.Command("remove", "Remove snapshot policy for a single directory, user@host or a global policy.").Alias("rm").Alias("delete")
	policyRemoveTargets = policyRemoveCommand.Arg("target", "Target of a policy ('global','user@host','@host') or a path").Strings()
	policyRemoveGlobal  = policyRemoveCommand.Flag("global", "Set global policy").Bool()
	policyRemoveDryRun  = policyRemoveCommand.Flag("dry-run", "Do not remove").Short('n').Bool()
)

func init() {
	policyRemoveCommand.Action(repositoryAction(removePolicy))
}

func removePolicy(ctx context.Context, rep repo.Repository) error {
	targets, err := policyTargets(ctx, rep, policyRemoveGlobal, policyRemoveTargets)
	if err != nil {
		return err
	}

	for _, target := range targets {
		log(ctx).Infof("Removing policy on %q...", target)

		if *policyRemoveDryRun {
			continue
		}

		if err := policy.RemovePolicy(ctx, rep, target); err != nil {
			return err
		}
	}

	return nil
}
