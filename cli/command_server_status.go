package cli

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

type commandServerStatus struct {
	sf serverClientFlags
}

func (c *commandServerStatus) setup(parent commandParent) {
	cmd := parent.Command("status", "Status of Kopia server")

	c.sf.setup(cmd)

	cmd.Action(serverAction(&c.sf, c.runServerStatus))
}

func (c *commandServerStatus) runServerStatus(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	var status serverapi.SourcesResponse
	if err := cli.Get(ctx, "sources", nil, &status); err != nil {
		return errors.Wrap(err, "unable to list sources")
	}

	for _, src := range status.Sources {
		fmt.Printf("%15v %v\n", src.Status, src.Source)
	}

	return nil
}
