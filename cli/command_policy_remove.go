package cli

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

var (
	policyRemoveCommand = policyCommands.Command("remove", "Remove snapshot policy for a single directory, user@host or a global policy.").Alias("rm").Alias("delete")
	policyRemoveTargets = policyRemoveCommand.Arg("target", "Target of a policy ('global','user@host','@host') or a path").Strings()
	policyRemoveGlobal  = policyRemoveCommand.Flag("global", "Set global policy").Bool()
)

func init() {
	policyRemoveCommand.Action(repositoryAction(removePolicy))
}

func removePolicy(ctx context.Context, rep *repo.Repository) error {
	mgr := snapshot.NewPolicyManager(rep)

	targets, err := policyTargets(mgr, policyRemoveGlobal, policyRemoveTargets)
	if err != nil {
		return err
	}

	for _, target := range targets {
		log.Printf("Removing policy on %q...", target)
		if err := mgr.RemovePolicy(target); err != nil {
			return err
		}
	}

	return nil
}
