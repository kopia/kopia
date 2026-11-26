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

	cw := &contentWorkload{
		written: map[content.ID][]byte{},
		deleted: map[content.ID]bool{},
	}

	// the epoch advances about every second iteration
	for epochIndex := range 80 {
		cw.simulateEpoch(ctx, t, w, clock, epochIndex)

		clock.Advance(25 * time.Hour)

		require.NoError(t, maintenance.RunExclusive(ctx, w, maintenance.ModeFull, true, func(ctx context.Context, runParams maintenance.RunParameters) error {
			return maintenance.Run(ctx, runParams, safety)
		}))

		if epochIndex%20 == 19 {
			verifyContents(ctx, t, env, clock, cw)
		}
	}

	s, err := maintenance.GetSchedule(ctx, w)
	require.NoError(t, err)

	var dropped, consolidations uint64

	for _, cs := range runStats[*maintenancestats.ConsolidateRangeCheckpointsStats](t, s.Runs[maintenance.TaskEpochConsolidateRanges]) {
		if cs.OutputBlobCount > 0 {
			consolidations++
			dropped += cs.DroppedDeletedEntries
		}
	}

	require.Positive(t, consolidations, "range checkpoints must have been consolidated")
	require.Positive(t, dropped, "deleted entries must have been dropped")

	var deletedSuperseded uint64

	for _, cs := range runStats[*maintenancestats.CleanupSupersededIndexesStats](t, s.Runs[maintenance.TaskEpochDeleteSupersededRanges]) {
		deletedSuperseded += cs.DeletedBlobCount
	}

	require.Positive(t, deletedSuperseded, "superseded range checkpoints must have been deleted")

	t.Logf("%v consolidations dropped %v deleted entries, %v superseded range checkpoint blobs deleted", consolidations, dropped, deletedSuperseded)

	requireOnlyChainAndRecentRanges(ctx, t, env, clock, params.RangeCheckpointConsolidation.MinRangeCheckpoints)
	verifyContents(ctx, t, env, clock, cw)
}

// contentWorkload writes contents and deletes most of them 3 epochs later.
type contentWorkload struct {
	written map[content.ID][]byte
	deleted map[content.ID]bool
	byEpoch [][]content.ID
}

func (cw *contentWorkload) simulateEpoch(ctx context.Context, t *testing.T, w repo.DirectRepositoryWriter, clock *faketime.ClockTimeWithOffset, epochIndex int) {
	t.Helper()

	var ids []content.ID

	// 21 flushes create 21 index blobs, which advances the epoch
	for round := range 21 {
		for k := range 3 {
			data := fmt.Appendf(nil, "content-%d-%d-%d", epochIndex, round, k)

			id, err := w.ContentManager().WriteContent(ctx, gather.FromSlice(data), "", content.NoCompression)
			require.NoError(t, err)

			cw.written[id] = data
			ids = append(ids, id)
		}

		if round == 0 && epochIndex >= 3 {
			cw.deleteMost(ctx, t, w, cw.byEpoch[epochIndex-3])
		}

		require.NoError(t, w.Flush(ctx))
		clock.Advance(time.Hour)
	}

	cw.byEpoch = append(cw.byEpoch, ids)
}

func (cw *contentWorkload) deleteMost(ctx context.Context, t *testing.T, w repo.DirectRepositoryWriter, ids []content.ID) {
	t.Helper()

	for n, id := range ids {
		if n%3 != 0 {
			require.NoError(t, w.ContentManager().DeleteContent(ctx, id))

			cw.deleted[id] = true
		}
	}
}

// runStats returns the statistics of type T recorded by the given maintenance runs, which must all have succeeded.
func runStats[T maintenancestats.Summarizer](t *testing.T, runs []maintenance.RunInfo) []T {
	t.Helper()

	var result []T

	for _, run := range runs {
		require.True(t, run.Success, run.Error)

		for _, extra := range run.Extra {
			summary, err := maintenancestats.BuildFromExtra(extra)
			require.NoError(t, err)

			if s, ok := summary.(T); ok {
				result = append(result, s)
			}
		}
	}

	return result
}

// requireOnlyChainAndRecentRanges checks that the chain is short and that superseded range checkpoints were deleted.
func requireOnlyChainAndRecentRanges(ctx context.Context, t *testing.T, env *repotesting.Environment, clock *faketime.ClockTimeWithOffset, minRangeCheckpoints int) {
	t.Helper()

	em, ok, err := env.RepositoryWriter.ContentManager().EpochManager(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, em.Refresh(ctx))

	cs, err := em.Current(ctx)
	require.NoError(t, err)
	require.Less(t, len(cs.LongestRangeCheckpointSets), minRangeCheckpoints)

	chainBlobs := map[blob.ID]bool{}

	for _, r := range cs.LongestRangeCheckpointSets {
		for _, bm := range r.Blobs {
			chainBlobs[bm.BlobID] = true
		}
	}

	allRangeBlobs, err := blob.ListAllBlobs(ctx, env.RootStorage(), epoch.RangeCheckpointIndexBlobPrefix)
	require.NoError(t, err)

	// only range checkpoints superseded within the safety margin may remain
	for _, bm := range allRangeBlobs {
		if !chainBlobs[bm.BlobID] {
			require.Less(t, clock.NowFunc()().Sub(bm.Timestamp), 4*24*time.Hour, "superseded range checkpoint %v was not deleted", bm.BlobID)
		}
	}
}

// verifyContents opens the repository like a new client and checks every content ever written.
func verifyContents(ctx context.Context, t *testing.T, env *repotesting.Environment, clock *faketime.ClockTimeWithOffset, cw *contentWorkload) {
	t.Helper()

	rep, err := repo.Open(ctx, env.ConfigFile(), env.Password, &repo.Options{TimeNowFunc: clock.NowFunc()})
	require.NoError(t, err)

	defer rep.Close(ctx) //nolint:errcheck

	dr, ok := rep.(repo.DirectRepository)
	require.True(t, ok)

	for id, data := range cw.written {
		info, err := dr.ContentInfo(ctx, id)

		if cw.deleted[id] {
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
