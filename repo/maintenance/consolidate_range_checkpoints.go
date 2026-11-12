package maintenance

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/repo/content/indexblob"
	"github.com/kopia/kopia/repo/maintenancestats"
)

// runTaskConsolidateRangeCheckpoints merges the range checkpoint chain into one checkpoint once it is long enough,
// and deletes range checkpoints superseded by an earlier consolidation.
func runTaskConsolidateRangeCheckpoints(ctx context.Context, em *epoch.Manager, runParams RunParameters, s *Schedule) error {
	cp := runParams.Params.RangeCheckpointConsolidation

	if err := reportRunAndMaybeCheckContentIndex(ctx, runParams.rep, TaskEpochConsolidateRanges, s, func() (maintenancestats.Kind, error) {
		return consolidateRangeCheckpoints(ctx, em, runParams, cp)
	}); err != nil {
		return err
	}

	return reportRunAndMaybeCheckContentIndex(ctx, runParams.rep, TaskEpochDeleteSupersededRanges, s, func() (maintenancestats.Kind, error) {
		userLog(ctx).Info("Cleaning up superseded range checkpoints...")

		stats, err := em.CleanupSupersededRangeCheckpoints(ctx)

		return stats, errors.Wrap(err, "error removing superseded range checkpoints")
	})
}

func consolidateRangeCheckpoints(ctx context.Context, em *epoch.Manager, runParams RunParameters, cp RangeCheckpointConsolidationParams) (maintenancestats.Kind, error) {
	cs, err := em.Current(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get epoch state")
	}

	if n, minLength := len(cs.LongestRangeCheckpointSets), cp.GetMinRangeCheckpoints(); n < minLength {
		userLog(ctx).Debugf("Not consolidating range checkpoints: chain has %v of %v checkpoints.", n, minLength)

		return nil, nil
	}

	userLog(ctx).Info("Consolidating range checkpoints...")

	stats, err := runParams.rep.ContentManager().IndexBlobManagerV1().MergeRangeCheckpoints(ctx, indexblob.MergeRangeOptions{
		MaxEntriesPerPartition: cp.MaxEntriesPerPartition,
		Progress:               userLog(ctx).Debugf,
	})
	if err != nil {
		return nil, errors.Wrap(err, "error consolidating range checkpoints")
	}

	return &maintenancestats.ConsolidateRangeCheckpointsStats{
		RangeMinEpoch:         maintenancestats.ToUint64(stats.MinEpoch),
		RangeMaxEpoch:         maintenancestats.ToUint64(stats.MaxEpoch),
		InputRangeCheckpoints: maintenancestats.ToUint64(stats.InputRanges),
		InputBlobCount:        maintenancestats.ToUint64(stats.InputBlobs),
		InputTotalSize:        maintenancestats.ToUint64(stats.InputBytes),
		InputEntries:          maintenancestats.ToUint64(stats.InputEntries),
		DroppedDeletedEntries: maintenancestats.ToUint64(stats.DroppedDeleted),
		OutputEntries:         maintenancestats.ToUint64(stats.OutputEntries),
		OutputBlobCount:       maintenancestats.ToUint64(len(stats.OutputBlobs)),
		OutputTotalSize:       maintenancestats.ToUint64(stats.OutputBytes),
	}, nil
}
