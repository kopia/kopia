package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
)

var (
	optimizeCommand        = blockIndexCommands.Command("optimize", "Optimize block indexes.")
	optimizeMinSmallBlocks = blockIndexCommands.Flag("min-small-blocks", "Minimum number of small blocks that can be left after compaction.").Default("1").Int()
	optimizeMaxSmallBlocks = blockIndexCommands.Flag("max-small-blocks", "Maximum number of small blocks that can be left after compaction.").Default("1").Int()
)

func runOptimizeCommand(ctx context.Context, rep *repo.Repository) error {
	return rep.Blocks.CompactIndexes(ctx, *optimizeMinSmallBlocks, *optimizeMaxSmallBlocks)
}

func init() {
	optimizeCommand.Action(repositoryAction(runOptimizeCommand))
}
