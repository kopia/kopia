package cli

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

var (
	optimizeCommand              = indexCommands.Command("optimize", "Optimize indexes blobs.")
	optimizeMinSmallBlobs        = optimizeCommand.Flag("min-small-blobs", "Minimum number of small index blobs that can be left after compaction.").Default("1").Int()
	optimizeMaxSmallBlobs        = optimizeCommand.Flag("max-small-blobs", "Maximum number of small index blobs that can be left after compaction.").Default("1").Int()
	optimizeSkipDeletedOlderThan = optimizeCommand.Flag("skip-deleted-older-than", "Skip deleted blobs above given age").Duration()
	optimizeAllIndexes           = optimizeCommand.Flag("all", "Optimize all indexes, even those above maximum size.").Bool()
)

func runOptimizeCommand(ctx context.Context, rep *repo.Repository) error {
	return rep.Content.CompactIndexes(ctx, content.CompactOptions{
		MinSmallBlobs:        *optimizeMinSmallBlobs,
		MaxSmallBlobs:        *optimizeMaxSmallBlobs,
		AllIndexes:           *optimizeAllIndexes,
		SkipDeletedOlderThan: *optimizeSkipDeletedOlderThan,
	})
}

func init() {
	optimizeCommand.Action(repositoryAction(runOptimizeCommand))
}
