package cli

import (
	"context"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

var serverFlushCommand = serverCommands.Command("flush", "Flush the state of Kopia server to persistent storage, etc.")

func init() {
	serverFlushCommand.Action(serverAction(runServerFlush))
}

func runServerFlush(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	return cli.Post(ctx, "flush", &serverapi.Empty{}, &serverapi.Empty{})
}
