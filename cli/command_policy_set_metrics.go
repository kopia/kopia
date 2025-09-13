package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/snapshot/policy"
)

type policyMetricsFlags struct {
	policyExposeMetrics string
}

func (c *policyMetricsFlags) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("expose-metrics", "Expose metrics ('true', 'false', 'inherit')").EnumVar(&c.policyExposeMetrics, booleanEnumValues...)
}

func (c *policyMetricsFlags) setMetricsPolicyFromFlags(ctx context.Context, mp *policy.MetricsPolicy, changeCount *int) error {
	if err := applyPolicyBoolPtr(ctx, "expose metrics", &mp.ExposeMetrics, c.policyExposeMetrics, changeCount); err != nil {
		return errors.Wrap(err, "expose metrics")
	}

	return nil
}
