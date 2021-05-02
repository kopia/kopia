package cli

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

type commandServerUpload struct {
	sf serverClientFlags
}

func (c *commandServerUpload) setup(parent commandParent) {
	cmd := parent.Command("upload", "Trigger upload for one or more sources")
	c.sf.setup(cmd)
	cmd.Action(serverAction(&c.sf, c.run))
}

func (c *commandServerUpload) run(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	return triggerActionOnMatchingSources(ctx, cli, "sources/upload")
}

func triggerActionOnMatchingSources(ctx context.Context, cli *apiclient.KopiaAPIClient, path string) error {
	var resp serverapi.MultipleSourceActionResponse

	if err := cli.Post(ctx, path, &serverapi.Empty{}, &resp); err != nil {
		return errors.Wrapf(err, "unable to start upload on %v", path)
	}

	for src, resp := range resp.Sources {
		if resp.Success {
			fmt.Println("SUCCESS", src)
		} else {
			fmt.Println("FAILED", src)
		}
	}

	return nil
}
