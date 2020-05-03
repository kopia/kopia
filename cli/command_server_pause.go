package cli

import (
	"context"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

var (
	serverPauseCommand = serverCommands.Command("pause", "Pause the scheduled snapshots for one or more sources")
)

func init() {
	serverPauseCommand.Action(serverAction(runServerPause))
}

func runServerPause(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	return cli.Post(ctx, "sources/pause", &serverapi.Empty{}, &serverapi.Empty{})
}
