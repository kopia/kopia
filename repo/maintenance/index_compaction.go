package maintenance

import (
	"context"

	"github.com/kopia/kopia/repo/content/indexblob"
)

// runTaskIndexCompactionQuick rewrites index blobs to reduce their count but does not drop any contents.
func runTaskIndexCompactionQuick(ctx context.Context, runParams RunParameters, s *Schedule, safety SafetyParameters) error {
	return ReportRun(ctx, runParams.rep, TaskIndexCompaction, s, func() error {
		log(ctx).Info("Compacting indexes...")

		const maxSmallBlobsForIndexCompaction = 8

		return runParams.rep.ContentManager().CompactIndexes(ctx, indexblob.CompactOptions{
			MaxSmallBlobs:                    maxSmallBlobsForIndexCompaction,
			DisableEventualConsistencySafety: safety.DisableEventualConsistencySafety,
		})
	})
}
