package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/repo/blob/throttling"
)

type commandServerThrottleGet struct {
	sf serverClientFlags

	ctg commonThrottleGet
}

func (c *commandServerThrottleGet) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("get", "Get throttling parameters for a running server")
	c.sf.setup(svc, cmd)
	c.ctg.setup(svc, cmd)
	cmd.Action(svc.serverAction(&c.sf, c.run))
}

func (c *commandServerThrottleGet) run(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	var limits throttling.Limits

	if err := cli.Get(ctx, "control/throttle", nil, &limits); err != nil {
		return errors.Wrap(err, "unable to get current throttle")
	}

	return c.ctg.output(&limits)
}
