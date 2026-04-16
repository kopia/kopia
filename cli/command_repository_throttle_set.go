package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandRepositoryThrottleSet struct {
	cts commonThrottleSet
}

func (c *commandRepositoryThrottleSet) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("set", "Set throttling parameters for a repository")
	c.cts.setup(cmd)

	cmd.Action(svc.directRepositoryWriteAction(c.run))
}

func (c *commandRepositoryThrottleSet) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	thr := rep.Throttler()
	limits := thr.Limits()

	var changeCount int

	if err := c.cts.apply(ctx, &limits, &changeCount); err != nil {
		return err
	}

	if changeCount == 0 {
		log(ctx).Info("No changes made.")
		return nil
	}

	return errors.Wrap(rep.Throttler().SetLimits(limits), "error setting limits")
}
