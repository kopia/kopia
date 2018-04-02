package cli

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

var (
	policyListCommand = policyCommands.Command("list", "List policies.").Alias("ls")
)

func init() {
	policyListCommand.Action(repositoryAction(listPolicies))
}

func listPolicies(ctx context.Context, rep *repo.Repository) error {
	mgr := snapshot.NewPolicyManager(rep)

	policies, err := mgr.ListPolicies()
	if err != nil {
		return err
	}

	for _, pol := range policies {
		fmt.Println(pol.Labels)
	}

	return nil
}
