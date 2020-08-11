package cli

import (
	"context"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

var serverRefreshCommand = serverCommands.Command("refresh", "Refresh the cache in Kopia server to observe new sources, etc.")

func init() {
	serverRefreshCommand.Action(serverAction(runServerRefresh))
}

func runServerRefresh(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	return cli.Post(ctx, "refresh", &serverapi.Empty{}, &serverapi.Empty{})
}
