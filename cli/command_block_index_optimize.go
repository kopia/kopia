package cli

import (
	"context"

	"github.com/kopia/repo"
	"github.com/kopia/repo/block"
)

var (
	optimizeCommand              = blockIndexCommands.Command("optimize", "Optimize block indexes.")
	optimizeMinSmallBlocks       = optimizeCommand.Flag("min-small-blocks", "Minimum number of small index blocks that can be left after compaction.").Default("1").Int()
	optimizeMaxSmallBlocks       = optimizeCommand.Flag("max-small-blocks", "Maximum number of small index blocks that can be left after compaction.").Default("1").Int()
	optimizeSkipDeletedOlderThan = optimizeCommand.Flag("skip-deleted-older-than", "Skip deleted blocks above given age").Duration()
	optimizeAllBlocks            = optimizeCommand.Flag("all", "Optimize all indexes, even those above maximum size.").Bool()
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
