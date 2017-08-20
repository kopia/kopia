package cli

import (
	"fmt"

	"github.com/kopia/kopia/snapshot"
)

func policyTargets(globalFlag *bool, targetsFlag *[]string) ([]*snapshot.SourceInfo, error) {
	if *globalFlag == (len(*targetsFlag) > 0) {
		return nil, fmt.Errorf("must pass either '--global' or a list of path targets")
	}

	if *globalFlag {
		return []*snapshot.SourceInfo{
			snapshot.GlobalPolicySourceInfo,
		}, nil
	}

	var res []*snapshot.SourceInfo
	for _, ts := range *targetsFlag {
		target, err := snapshot.ParseSourceInfo(ts, getHostName(), getUserName())
		if err != nil {
			return nil, err
		}

		res = append(res, &target)
	}

	return res, nil
}
