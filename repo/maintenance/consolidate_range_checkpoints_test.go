package maintenance_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/maintenancestats"
)

// TestRangeCheckpointConsolidation runs full maintenance with range checkpoint consolidation over many epochs
// of writes and deletions and checks that readers always see the same contents.
func TestRangeCheckpointConsolidation(t *testing.T) {
	t.Parallel()

	clock := faketime.NewClockTimeWithOffset(0)
	openOpt := func(o *repo.Options) { o.TimeNowFunc = clock.NowFunc() }

	ctx, env := repotesting.NewEnvironment(t, format.FormatVersion3, repotesting.Options{OpenOptions: openOpt})
	w := env.RepositoryWriter

	params, err := maintenance.GetParams(ctx, w)
	require.NoError(t, err)

	params.Owner = w.ClientOptions().UsernameAtHost()
	params.RangeCheckpointConsolidation = maintenance.RangeCheckpointConsolidationParams{
		Enabled:                true,
		MinRangeCheckpoints:    3,
		MaxEntriesPerPartition: 40,
	}

	require.NoError(t, maintenance.SetParams(ctx, w, params))
	require.NoError(t, w.Flush(ctx))

	// rewriting the (small) packs would give every live content a new index entry and hide lost entries
	safety := maintenance.SafetyNone
	safety.RewriteMinAge = 365 * 24 * time.Hour

	var (
		written = map[content.ID][]byte{}
		deleted = map[content.ID]bool{}
		byEpoch [][]content.ID
	)

	// the epoch advances about every second iteration
	for epochIndex := range 80 {
		var ids []content.ID

		// 21 flushes create 21 index blobs, which advances the epoch
		for round := range 21 {
			for k := range 3 {
				data := fmt.Appendf(nil, "content-%d-%d-%d", epochIndex, round, k)

				id, err := w.ContentManager().WriteContent(ctx, gather.FromSlice(data), "", content.NoCompression)
				require.NoError(t, err)

				written[id] = data
				ids = append(ids, id)
			}

			if round == 0 && epochIndex >= 3 {
				for n, id := range byEpoch[epochIndex-3] {
					if n%3 != 0 {
						require.NoError(t, w.ContentManager().DeleteContent(ctx, id))

						deleted[id] = true
					}
				}
			}

			require.NoError(t, w.Flush(ctx))
			clock.Advance(time.Hour)
		}

		byEpoch = append(byEpoch, ids)

		clock.Advance(25 * time.Hour)

		require.NoError(t, maintenance.RunExclusive(ctx, w, maintenance.ModeFull, true, func(ctx context.Context, runParams maintenance.RunParameters) error {
			return maintenance.Run(ctx, runParams, safety)
		}))

		if epochIndex%20 == 19 {
			verifyContents(ctx, t, env, clock, written, deleted)
		}
	}

	s, err := maintenance.GetSchedule(ctx, w)
	require.NoError(t, err)

	var dropped, consolidations uint64

	for _, run := range s.Runs[maintenance.TaskEpochConsolidateRanges] {
		require.True(t, run.Success, run.Error)

		for _, extra := range run.Extra {
			summary, err := maintenancestats.BuildFromExtra(extra)
			require.NoError(t, err)

			if cs, ok := summary.(*maintenancestats.ConsolidateRangeCheckpointsStats); ok && cs.OutputBlobCount > 0 {
				consolidations++
				dropped += cs.DroppedDeletedEntries
			}
		}
	}

	require.Positive(t, consolidations, "range checkpoints must have been consolidated")
	require.Positive(t, dropped, "deleted entries must have been dropped")

	var deletedSuperseded uint64

	for _, run := range s.Runs[maintenance.TaskEpochDeleteSupersededRanges] {
		require.True(t, run.Success, run.Error)

		for _, extra := range run.Extra {
			summary, err := maintenancestats.BuildFromExtra(extra)
			require.NoError(t, err)

			if cs, ok := summary.(*maintenancestats.CleanupSupersededIndexesStats); ok {
				deletedSuperseded += cs.DeletedBlobCount
			}
		}
	}

	require.Positive(t, deletedSuperseded, "superseded range checkpoints must have been deleted")

	t.Logf("%v consolidations dropped %v deleted entries, %v superseded range checkpoint blobs deleted", consolidations, dropped, deletedSuperseded)

	em, ok, err := w.ContentManager().EpochManager(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, em.Refresh(ctx))

	cs, err := em.Current(ctx)
	require.NoError(t, err)
	require.Less(t, len(cs.LongestRangeCheckpointSets), params.RangeCheckpointConsolidation.MinRangeCheckpoints)

	// only the chain and range checkpoints within the safety margin may remain
	chainBlobs := map[blob.ID]bool{}

	for _, r := range cs.LongestRangeCheckpointSets {
		for _, bm := range r.Blobs {
			chainBlobs[bm.BlobID] = true
		}
	}

	allRangeBlobs, err := blob.ListAllBlobs(ctx, env.RootStorage(), epoch.RangeCheckpointIndexBlobPrefix)
	require.NoError(t, err)

	for _, bm := range allRangeBlobs {
		if !chainBlobs[bm.BlobID] {
			require.Less(t, clock.NowFunc()().Sub(bm.Timestamp), 4*24*time.Hour, "superseded range checkpoint %v was not deleted", bm.BlobID)
		}
	}

	verifyContents(ctx, t, env, clock, written, deleted)
}

// verifyContents opens the repository like a new client and checks every content ever written.
func verifyContents(ctx context.Context, t *testing.T, env *repotesting.Environment, clock *faketime.ClockTimeWithOffset, written map[content.ID][]byte, deleted map[content.ID]bool) {
	t.Helper()

	rep, err := repo.Open(ctx, env.ConfigFile(), env.Password, &repo.Options{TimeNowFunc: clock.NowFunc()})
	require.NoError(t, err)

	defer rep.Close(ctx) //nolint:errcheck

	dr, ok := rep.(repo.DirectRepository)
	require.True(t, ok)

	for id, data := range written {
		info, err := dr.ContentInfo(ctx, id)

		if deleted[id] {
			if err == nil {
				require.True(t, info.Deleted, "content %v must not be visible", id)
			} else {
				require.ErrorIs(t, err, content.ErrContentNotFound)
			}

			continue
		}

		require.NoError(t, err, "content %v must be visible", id)
		require.False(t, info.Deleted)

		got, err := dr.ContentReader().GetContent(ctx, id)
		require.NoError(t, err)
		require.Equal(t, data, got)
	}
}
