package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/block"
)

var (
	optimizeCommand              = blockIndexCommands.Command("optimize", "Optimize block indexes.")
	optimizeMinSmallBlocks       = blockIndexCommands.Flag("min-small-blocks", "Minimum number of small blocks that can be left after compaction.").Default("1").Int()
	optimizeMaxSmallBlocks       = blockIndexCommands.Flag("max-small-blocks", "Maximum number of small blocks that can be left after compaction.").Default("1").Int()
	optimizeSkipDeletedOlderThan = blockIndexCommands.Flag("skip-deleted-older-than", "Skip deleted blocks above given age").Duration()
	optimizeAllBlocks            = blockIndexCommands.Flag("all", "Optimize all blocks, even those above maximum size.").Bool()
)

func runOptimizeCommand(ctx context.Context, rep *repo.Repository) error {
	return rep.Blocks.CompactIndexes(ctx, block.CompactOptions{
		MinSmallBlocks:       *optimizeMinSmallBlocks,
		MaxSmallBlocks:       *optimizeMaxSmallBlocks,
		AllBlocks:            *optimizeAllBlocks,
		SkipDeletedOlderThan: *optimizeSkipDeletedOlderThan,
	})
}

func init() {
	optimizeCommand.Action(repositoryAction(runOptimizeCommand))
}
