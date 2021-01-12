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
	policyListCommand.Action(repositoryReaderAction(listPolicies))
}

func listPolicies(ctx context.Context, rep repo.Reader) error {
	policies, err := policy.ListPolicies(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "error listing policies")
	}

	sort.Slice(policies, func(i, j int) bool {
		return policies[i].Target().String() < policies[j].Target().String()
	})

	for _, pol := range policies {
		fmt.Println(pol.ID(), pol.Target())
	}

	return nil
}
