package maintenance

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

const maxSmallBlobsForIndexCompaction = 8

// IndexCompaction rewrites index blobs to reduce their count but does not drop any contents.
func IndexCompaction(ctx context.Context, rep repo.DirectRepositoryWriter, safety SafetyParameters) error {
	log(ctx).Infof("Compacting indexes...")

	// nolint:wrapcheck
	return rep.ContentManager().CompactIndexes(ctx, content.CompactOptions{
		MaxSmallBlobs:                    maxSmallBlobsForIndexCompaction,
		DisableEventualConsistencySafety: safety.DisableEventualConsistencySafety,
	})
}
