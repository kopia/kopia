package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

func policyTargets(ctx context.Context, rep repo.Repository, globalFlag *bool, targetsFlag *[]string) ([]snapshot.SourceInfo, error) {
	if *globalFlag == (len(*targetsFlag) > 0) {
		return nil, errors.New("must pass either '--global' or a list of path targets")
	}

	if *globalFlag {
		return []snapshot.SourceInfo{
			policy.GlobalPolicySourceInfo,
		}, nil
	}

	var res []snapshot.SourceInfo

	for _, ts := range *targetsFlag {
		// try loading policy by its manifest ID
		if t, err := policy.GetPolicyByID(ctx, rep, manifest.ID(ts)); err == nil {
			res = append(res, t.Target())
			continue
		}

		target, err := snapshot.ParseSourceInfo(ts, rep.Hostname(), rep.Username())
		if err != nil {
			return nil, err
		}

		res = append(res, target)
	}

	return res, nil
}
