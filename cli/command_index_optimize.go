package cli

import (
	"context"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

type commandIndexOptimize struct {
	optimizeMaxSmallBlobs        int
	optimizeDropDeletedOlderThan time.Duration
	optimizeDropContents         []string
	optimizeAllIndexes           bool

	svc appServices
}

func (c *commandIndexOptimize) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("optimize", "Optimize indexes blobs.")
	cmd.Flag("max-small-blobs", "Maximum number of small index blobs that can be left after compaction.").Default("1").IntVar(&c.optimizeMaxSmallBlobs)
	cmd.Flag("drop-deleted-older-than", "Drop deleted contents above given age").DurationVar(&c.optimizeDropDeletedOlderThan)
	cmd.Flag("drop-contents", "Drop contents with given IDs").StringsVar(&c.optimizeDropContents)
	cmd.Flag("all", "Optimize all indexes, even those above maximum size.").BoolVar(&c.optimizeAllIndexes)
	cmd.Action(svc.directRepositoryWriteAction(c.runOptimizeCommand))

	c.svc = svc
}

func (c *commandIndexOptimize) runOptimizeCommand(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	c.svc.advancedCommand(ctx)

	opt := content.CompactOptions{
		MaxSmallBlobs: c.optimizeMaxSmallBlobs,
		AllIndexes:    c.optimizeAllIndexes,
		DropContents:  toContentIDs(c.optimizeDropContents),
	}

	if age := c.optimizeDropDeletedOlderThan; age > 0 {
		opt.DropDeletedBefore = rep.Time().Add(-age)
	}

	// nolint:wrapcheck
	return rep.ContentManager().CompactIndexes(ctx, opt)
}
