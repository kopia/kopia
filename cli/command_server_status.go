package cli

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

var (
	serverStatusCommand = serverCommands.Command("status", "Status of Kopia server")
)

func init() {
	serverStatusCommand.Action(serverAction(runServerStatus))
}

func runServerStatus(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	var status serverapi.SourcesResponse
	if err := cli.Get(ctx, "sources", nil, &status); err != nil {
		return err
	}

	for _, src := range status.Sources {
		fmt.Printf("%15v %v\n", src.Status, src.Source)
	}

	return nil
}
