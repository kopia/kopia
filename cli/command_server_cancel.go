package cli

import (
	"context"

	"github.com/kopia/kopia/internal/serverapi"
)

var (
	serverCancelUploadCommand = serverCommands.Command("cancel", "Cancels in-progress uploads for one or more sources")
)

func init() {
	serverCancelUploadCommand.Action(serverAction(runServerCancelUpload))
}

func runServerCancelUpload(ctx context.Context, cli *serverapi.Client) error {
	return cli.Post(ctx, "sources/cancel", &serverapi.Empty{}, &serverapi.Empty{})
}
