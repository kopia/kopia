package cli

import (
	"context"
	"sort"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot/policy"
)

type commandPolicyList struct {
	jo  jsonOutput
	out textOutput
}

func (c *commandPolicyList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List policies.").Alias("ls")
	c.jo.setup(svc, cmd)
	c.out.setup(svc)
	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandPolicyList) run(ctx context.Context, rep repo.Repository) error {
	var jl jsonList

	jl.begin(&c.jo)
	defer jl.end()

	policies, err := policy.ListPolicies(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "error listing policies")
	}

	sort.Slice(policies, func(i, j int) bool {
		return policies[i].Target().String() < policies[j].Target().String()
	})

	for _, pol := range policies {
		if c.jo.jsonOutput {
			jl.emit(policy.TargetWithPolicy{ID: pol.ID(), Target: pol.Target(), Policy: pol})
		} else {
			c.out.printStdout("%v %v\n", pol.ID(), pol.Target())
		}
	}

	return nil
}
