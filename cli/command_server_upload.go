package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

type commandServerUpload struct {
	sf serverClientFlags

	out textOutput
}

func (c *commandServerUpload) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("upload", "Trigger upload for one or more sources")
	c.sf.setup(cmd)
	c.out.setup(svc)
	cmd.Action(svc.serverAction(&c.sf, c.run))
}

func (c *commandServerUpload) run(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	return c.triggerActionOnMatchingSources(ctx, cli, "sources/upload")
}

func (c *commandServerUpload) triggerActionOnMatchingSources(ctx context.Context, cli *apiclient.KopiaAPIClient, path string) error {
	var resp serverapi.MultipleSourceActionResponse

	if err := cli.Post(ctx, path, &serverapi.Empty{}, &resp); err != nil {
		return errors.Wrapf(err, "unable to start upload on %v", path)
	}

	for src, resp := range resp.Sources {
		if resp.Success {
			c.out.printStdout("SUCCESS %v\n", src)
		} else {
			c.out.printStdout("FAILED %v\n", src)
		}
	}

	return nil
}
