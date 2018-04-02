package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

var (
	blockRepackCommand       = blockCommands.Command("repack", "Repackage small blocks into bigger ones")
	blockRepackSizeThreshold = blockRepackCommand.Flag("max-size", "Max size of block to re-pack").Default("500000").Uint64()
)

func runBlockRepackAction(ctx context.Context, rep *repo.Repository) error {
	return rep.Blocks.Repackage(ctx, *blockRepackSizeThreshold)
}

func init() {
	blockRepackCommand.Action(repositoryAction(runBlockRepackAction))
}
