package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

type commandServerStatus struct {
	sf serverClientFlags

	out textOutput

	remote bool
}

func (c *commandServerStatus) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("status", "Status of Kopia server")

	cmd.Flag("remote", "Show remote sources").BoolVar(&c.remote)

	c.sf.setup(svc, cmd)
	c.out.setup(svc)

	cmd.Action(svc.serverAction(&c.sf, c.runServerStatus))
}

func (c *commandServerStatus) runServerStatus(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	var status serverapi.SourcesResponse
	if err := cli.Get(ctx, "control/sources", nil, &status); err != nil {
		return errors.Wrap(err, "unable to list sources")
	}

	for _, src := range status.Sources {
		if src.Status == "REMOTE" && !c.remote {
			continue
		}

		c.out.printStdout("%v: %v\n", src.Status, src.Source)
	}

	return nil
}
