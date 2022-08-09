package maintenance

import (
	"context"

	"github.com/kopia/kopia/repo/content"
)

// runTaskIndexCompactionQuick rewrites index blobs to reduce their count but does not drop any contents.
func runTaskIndexCompactionQuick(ctx context.Context, runParams RunParameters, s *Schedule, safety SafetyParameters) error {
	return ReportRun(ctx, runParams.rep, TaskIndexCompaction, s, func() error {
		log(ctx).Infof("Compacting indexes...")

		const maxSmallBlobsForIndexCompaction = 8

		//nolint:wrapcheck
		return runParams.rep.ContentManager().CompactIndexes(ctx, content.CompactOptions{
			MaxSmallBlobs:                    maxSmallBlobsForIndexCompaction,
			DisableEventualConsistencySafety: safety.DisableEventualConsistencySafety,
		})
	})
}
