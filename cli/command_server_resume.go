package cli

import (
	"context"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

var serverResumeCommand = serverCommands.Command("resume", "Resume the scheduled snapshots for one or more sources")

func init() {
	serverResumeCommand.Action(serverAction(runServerResume))
}

func runServerResume(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	return cli.Post(ctx, "sources/resume", &serverapi.Empty{}, &serverapi.Empty{})
}
