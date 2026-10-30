package cli

import (
	"context"
	"sort"

	"github.com/alecthomas/kingpin/v2"

	"github.com/kopia/kopia/repo/splitter"
	"github.com/kopia/kopia/snapshot/policy"
)

type policySplitterFlags struct {
	policySetSplitterAlgorithmOverride string
}

func (c *policySplitterFlags) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("splitter", "Splitter algorithm override").EnumVar(&c.policySetSplitterAlgorithmOverride, supportedSplitterAlgorithms()...)
}

//nolint:unparam
func (c *policySplitterFlags) setSplitterPolicyFromFlags(ctx context.Context, p *policy.SplitterPolicy, changeCount *int) error {
	if v := c.policySetSplitterAlgorithmOverride; v != "" {
		if v == inheritPolicyString {
			log(ctx).Info(" - resetting splitter algorithm override to default value inherited from parent")

			p.Algorithm = ""
		} else {
			log(ctx).Infof(" - setting splitter algorithm override to %v", v)

			p.Algorithm = v
		}

		*changeCount++
	}

	return nil
}

func supportedSplitterAlgorithms() []string {
	res := append([]string{inheritPolicyString}, splitter.SupportedAlgorithms()...)

	sort.Strings(res)

	return res
}
