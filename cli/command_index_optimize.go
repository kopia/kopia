package cli

import (
	"context"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

var (
	optimizeCommand              = indexCommands.Command("optimize", "Optimize indexes blobs.")
	optimizeMaxSmallBlobs        = optimizeCommand.Flag("max-small-blobs", "Maximum number of small index blobs that can be left after compaction.").Default("1").Int()
	optimizeDropDeletedOlderThan = optimizeCommand.Flag("drop-deleted-older-than", "Drop deleted contents above given age").Duration()
	optimizeDropContents         = optimizeCommand.Flag("drop-contents", "Drop contents with given IDs").Strings()
	optimizeAllIndexes           = optimizeCommand.Flag("all", "Optimize all indexes, even those above maximum size.").Bool()
)

func runOptimizeCommand(ctx context.Context, rep *repo.DirectRepository) error {
	opt := content.CompactOptions{
		MaxSmallBlobs: *optimizeMaxSmallBlobs,
		AllIndexes:    *optimizeAllIndexes,
		DropContents:  toContentIDs(*optimizeDropContents),
	}

	if age := *optimizeDropDeletedOlderThan; age > 0 {
		opt.DropDeletedBefore = time.Now().Add(-age)
	}

	return rep.Content.CompactIndexes(ctx, opt)
}

func init() {
	optimizeCommand.Action(directRepositoryAction(runOptimizeCommand))
}
