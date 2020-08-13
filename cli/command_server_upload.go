package cli

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

var serverStartUploadCommand = serverCommands.Command("upload", "Trigger upload for one or more sources")

func init() {
	serverStartUploadCommand.Action(serverAction(runServerStartUpload))
}

func runServerStartUpload(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	return triggerActionOnMatchingSources(ctx, cli, "sources/upload")
}

func triggerActionOnMatchingSources(ctx context.Context, cli *apiclient.KopiaAPIClient, path string) error {
	var resp serverapi.MultipleSourceActionResponse

	if err := cli.Post(ctx, path, &serverapi.Empty{}, &resp); err != nil {
		return err
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
