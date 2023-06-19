package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandRepositoryThrottleGet struct {
	ctg commonThrottleGet
}

func (c *commandRepositoryThrottleGet) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("get", "Get throttling parameters for a repository")
	c.ctg.setup(svc, cmd)

	cmd.Action(svc.directRepositoryReadAction(c.run))
}

func (c *commandRepositoryThrottleGet) run(ctx context.Context, rep repo.DirectRepository) error {
	limits := rep.Throttler().Limits()

	if err := c.ctg.output(&limits); err != nil {
		return errors.Wrap(err, "output")
	}

	return nil
}
