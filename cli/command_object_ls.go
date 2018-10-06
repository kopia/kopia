package cli

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/repo"
)

var (
	objectListCommand = objectCommands.Command("list", "List objects").Alias("ls")
	objectListPrefix  = objectListCommand.Flag("prefix", "Prefix").String()
)

func runListObjectsAction(ctx context.Context, rep *repo.Repository) error {
	info, err := rep.Blocks.ListBlockInfos(*objectListPrefix, false)
	if err != nil {
		return err
	}

	for _, b := range info {
		fmt.Printf("D%-34v %10v %v\n", b.BlockID, b.Length, formatTimestamp(b.Timestamp()))
	}

	return nil
}

func init() {
	objectListCommand.Action(repositoryAction(runListObjectsAction))
}
