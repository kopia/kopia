package cli

import (
	"context"
	"fmt"

	"github.com/kopia/repo"
	"github.com/kopia/repo/storage"
)

var (
	storageListCommand = storageCommands.Command("list", "List storage blocks").Alias("ls")
	storageListPrefix  = storageListCommand.Flag("prefix", "Block prefix").String()
	storageListMinSize = storageListCommand.Flag("min-size", "Minimum size").Int64()
	storageListMaxSize = storageListCommand.Flag("max-size", "Maximum size").Int64()
)

func runListStorageBlocks(ctx context.Context, rep *repo.Repository) error {
	return rep.Storage.ListBlocks(ctx, *storageListPrefix, func(b storage.BlockMetadata) error {
		if *storageListMaxSize != 0 && b.Length > *storageListMaxSize {
			return nil
		}

		if *storageListMinSize != 0 && b.Length < *storageListMinSize {
			return nil
		}

		fmt.Printf("%-70v %10v %v\n", b.BlockID, b.Length, formatTimestamp(b.Timestamp))
		return nil
	})
}

func init() {
	storageListCommand.Action(repositoryAction(runListStorageBlocks))
}
