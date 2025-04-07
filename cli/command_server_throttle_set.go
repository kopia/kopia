package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/repo/blob/throttling"
)

type commandServerThrottleSet struct {
	sf serverClientFlags

	cts commonThrottleSet
}

func (c *commandServerThrottleSet) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("set", "Set throttling parameters for a running server")
	c.sf.setup(svc, cmd)
	c.cts.setup(cmd)

	cmd.Action(svc.serverAction(&c.sf, c.run))
}

func (c *commandServerThrottleSet) run(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	var limits throttling.Limits

	if err := cli.Get(ctx, "control/throttle", nil, &limits); err != nil {
		return errors.Wrap(err, "unable to get current throttle")
	}

	var changeCount int

	if err := c.cts.apply(ctx, &limits, &changeCount); err != nil {
		return err
	}

	if changeCount == 0 {
		log(ctx).Info("No changes made.")
		return nil
	}

	if err := cli.Put(ctx, "control/throttle", &limits, &serverapi.Empty{}); err != nil {
		return errors.Wrap(err, "unable to change throttle")
	}

	return nil
}
