package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

var (
	optimizeCommand              = indexCommands.Command("optimize", "Optimize indexes blobs.")
	optimizeMaxSmallBlobs        = optimizeCommand.Flag("max-small-blobs", "Maximum number of small index blobs that can be left after compaction.").Default("1").Int()
	optimizeSkipDeletedOlderThan = optimizeCommand.Flag("skip-deleted-older-than", "Skip deleted blobs above given age").Duration()
	optimizeAllIndexes           = optimizeCommand.Flag("all", "Optimize all indexes, even those above maximum size.").Bool()
)

func runOptimizeCommand(ctx context.Context, rep *repo.DirectRepository) error {
	return rep.Content.CompactIndexes(ctx, content.CompactOptions{
		MaxSmallBlobs:        *optimizeMaxSmallBlobs,
		AllIndexes:           *optimizeAllIndexes,
		SkipDeletedOlderThan: *optimizeSkipDeletedOlderThan,
	})
}

func init() {
	optimizeCommand.Action(directRepositoryAction(runOptimizeCommand))
}
