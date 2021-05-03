package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

type commandPolicy struct {
	edit   commandPolicyEdit
	list   commandPolicyList
	delete commandPolicyDelete
	set    commandPolicySet
	show   commandPolicyShow
}

func (c *commandPolicy) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("policy", "Commands to manipulate snapshotting policies.").Alias("policies")

	c.edit.setup(svc, cmd)
	c.list.setup(svc, cmd)
	c.delete.setup(svc, cmd)
	c.set.setup(svc, cmd)
	c.show.setup(svc, cmd)
}

func policyTargets(ctx context.Context, rep repo.Repository, globalFlag bool, targetsFlag []string) ([]snapshot.SourceInfo, error) {
	if globalFlag == (len(targetsFlag) > 0) {
		return nil, errors.New("must pass either '--global' or a list of path targets")
	}

	if globalFlag {
		return []snapshot.SourceInfo{
			policy.GlobalPolicySourceInfo,
		}, nil
	}

	var res []snapshot.SourceInfo

	for _, ts := range targetsFlag {
		// try loading policy by its manifest ID
		if t, err := policy.GetPolicyByID(ctx, rep, manifest.ID(ts)); err == nil {
			res = append(res, t.Target())
			continue
		}

		target, err := snapshot.ParseSourceInfo(ts, rep.ClientOptions().Hostname, rep.ClientOptions().Username)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to parse source info: %q", ts)
		}

		res = append(res, target)
	}

	return res, nil
}
