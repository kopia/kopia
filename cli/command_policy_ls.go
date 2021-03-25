package cli

import (
	"context"
	"fmt"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/policy"
)

var policyListCommand = policyCommands.Command("list", "List policies.").Alias("ls")

func init() {
	registerJSONOutputFlags(policyListCommand)
	policyListCommand.Action(repositoryReaderAction(listPolicies))
}

func listPolicies(ctx context.Context, rep repo.Repository) error {
	var jl jsonList

	jl.begin()
	defer jl.end()

	policies, err := policy.ListPolicies(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "error listing policies")
	}

	sort.Slice(policies, func(i, j int) bool {
		return policies[i].Target().String() < policies[j].Target().String()
	})

	for _, pol := range policies {
		if jsonOutput {
			jl.emit(policy.TargetWithPolicy{ID: pol.ID(), Target: pol.Target(), Policy: pol})
		} else {
			fmt.Println(pol.ID(), pol.Target())
		}
	}

	return nil
}
