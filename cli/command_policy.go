package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

type commandPolicy struct {
	edit    commandPolicyEdit
	list    commandPolicyList
	delete  commandPolicyDelete
	set     commandPolicySet
	show    commandPolicyShow
	export  commandPolicyExport
	pImport commandPolicyImport
}

func (c *commandPolicy) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("policy", "Commands to manipulate snapshotting policies.").Alias("policies")

	c.edit.setup(svc, cmd)
	c.list.setup(svc, cmd)
	c.delete.setup(svc, cmd)
	c.set.setup(svc, cmd)
	c.show.setup(svc, cmd)
	c.export.setup(svc, cmd)
	c.pImport.setup(svc, cmd)
}

type policyTargetFlags struct {
	targets []string
	global  bool
}

func (c *policyTargetFlags) setup(cmd *kingpin.CmdClause) {
	cmd.Arg("target", "Select a particular policy ('user@host','@host','user@host:path' or a local path). Use --global to target the global policy.").StringsVar(&c.targets)
	cmd.Flag("global", "Select the global policy.").BoolVar(&c.global)
}

func (c *policyTargetFlags) policyTargets(ctx context.Context, rep repo.Repository) ([]snapshot.SourceInfo, error) {
	if c.global == (len(c.targets) > 0) {
		return nil, errors.New("must pass either '--global' or a list of path targets")
	}

	if c.global {
		return []snapshot.SourceInfo{
			policy.GlobalPolicySourceInfo,
		}, nil
	}

	var res []snapshot.SourceInfo

	for _, ts := range c.targets {
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
