package maintenance

import (
	"context"

	"github.com/kopia/kopia/repo/content"
)

const maxSmallBlobsForIndexCompaction = 8

// IndexCompaction rewrites index blobs to reduce their count but does not drop any contents.
func IndexCompaction(ctx context.Context, rep MaintainableRepository) error {
	log(ctx).Infof("Compacting indexes...")

	return rep.ContentManager().CompactIndexes(ctx, content.CompactOptions{
		MaxSmallBlobs: maxSmallBlobsForIndexCompaction,
	})
}
