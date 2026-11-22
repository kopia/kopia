package maintenance_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/maintenance"
)

func (s *formatSpecificTestSuite) TestFullMaintenanceExpiredDeletedContentsInDamagedPacks(t *testing.T) {
	for _, damage := range []string{"missing", "truncated"} {
		t.Run(damage, func(t *testing.T) {
			s.testFullMaintenanceDamagedPack(t, damage, true)
		})
	}
}

func (s *formatSpecificTestSuite) TestFullMaintenanceLiveContentsInDamagedPacks(t *testing.T) {
	for _, damage := range []string{"missing", "truncated"} {
		t.Run(damage, func(t *testing.T) {
			s.testFullMaintenanceDamagedPack(t, damage, false)
		})
	}
}

func (s *formatSpecificTestSuite) testFullMaintenanceDamagedPack(t *testing.T, damage string, deleted bool) {
	t.Helper()

	ft := faketime.NewTimeAdvance(time.Date(2024, time.October, 18, 0, 0, 0, 0, time.UTC))
	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	var contentIDs []content.ID

	// Separate sessions produce three short packs, leaving two live packs to rewrite
	// after the expired deleted entry is removed.
	for i := range 3 {
		require.NoError(t, repo.DirectWriteSession(ctx, env.RepositoryWriter, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.DirectRepositoryWriter) error {
			cid, err := w.ContentManager().WriteContent(ctx, gather.FromSlice([]byte(fmt.Sprintf("content-%v", i))), "", 0)
			contentIDs = append(contentIDs, cid)

			return err
		}))
	}

	info, err := env.Repository.ContentInfo(ctx, contentIDs[0])
	require.NoError(t, err)

	var livePacks []blob.ID

	for _, cid := range contentIDs[1:] {
		ci, err := env.Repository.ContentInfo(ctx, cid)
		require.NoError(t, err)

		livePacks = append(livePacks, ci.PackBlobID)
	}

	ft.Advance(time.Hour)

	if deleted {
		require.NoError(t, repo.DirectWriteSession(ctx, env.RepositoryWriter, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.DirectRepositoryWriter) error {
			return w.ContentManager().DeleteContent(ctx, contentIDs[0])
		}))
	}

	// The two successful GC cycles make the deleted entry safe to forget using
	// the default safety margins, without running snapshot GC on the live fixtures.
	firstGC := ft.Advance(2 * time.Hour)
	secondGC := ft.Advance(maintenance.SafetyFull.MarginBetweenSnapshotGC + time.Hour)
	require.NoError(t, maintenance.SetSchedule(ctx, env.RepositoryWriter, &maintenance.Schedule{
		Runs: map[maintenance.TaskType][]maintenance.RunInfo{
			maintenance.TaskSnapshotGarbageCollection: {
				{Start: secondGC, End: secondGC, Success: true},
				{Start: firstGC, End: firstGC, Success: true},
			},
		},
	}))

	st := env.RootStorage()
	if damage == "missing" {
		require.NoError(t, st.DeleteBlob(ctx, info.PackBlobID))
	} else {
		require.NoError(t, st.PutBlob(ctx, info.PackBlobID, gather.FromSlice(nil), blob.PutOptions{}))
	}

	run := func() error {
		return maintenance.RunExclusive(ctx, env.RepositoryWriter, maintenance.ModeFull, true, func(ctx context.Context, rp maintenance.RunParameters) error {
			return maintenance.Run(ctx, rp, maintenance.SafetyFull)
		})
	}

	if !deleted {
		require.ErrorContains(t, run(), "failed to rewrite 1 contents")

		info, err = env.Repository.ContentInfo(ctx, contentIDs[0])
		require.NoError(t, err)
		require.False(t, info.Deleted)

		return
	}

	require.NoError(t, run())

	_, err = env.Repository.ContentInfo(ctx, contentIDs[0])
	require.ErrorIs(t, err, content.ErrContentNotFound)

	schedule, err := maintenance.GetSchedule(ctx, env.RepositoryWriter)
	require.NoError(t, err)
	require.NotEmpty(t, schedule.Runs[maintenance.TaskDropDeletedContentsFull])
	require.True(t, schedule.Runs[maintenance.TaskDropDeletedContentsFull][0].Success)
	require.NotEmpty(t, schedule.Runs[maintenance.TaskRewriteContentsFull])
	require.True(t, schedule.Runs[maintenance.TaskRewriteContentsFull][0].Success)

	for i, cid := range contentIDs[1:] {
		ci, err := env.Repository.ContentInfo(ctx, cid)
		require.NoError(t, err)
		require.False(t, ci.Deleted)
		require.NotEqual(t, livePacks[i], ci.PackBlobID)
	}

	if damage == "truncated" {
		verifyBlobExists(t, st, info.PackBlobID)
	}

	// Orphan deletion must still wait for clients to refresh their indexes.
	ft.Advance(maintenance.SafetyFull.PackDeleteMinAge + maintenance.SafetyFull.MinRewriteToOrphanDeletionDelay)
	require.NoError(t, run())
	verifyBlobNotFound(t, st, info.PackBlobID)

	schedule, err = maintenance.GetSchedule(ctx, env.RepositoryWriter)
	require.NoError(t, err)
	require.NotEmpty(t, schedule.Runs[maintenance.TaskDeleteOrphanedBlobsFull])
	require.True(t, schedule.Runs[maintenance.TaskDeleteOrphanedBlobsFull][0].Success)

	for i, cid := range contentIDs[1:] {
		data, err := env.RepositoryWriter.ContentManager().GetContent(ctx, cid)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("content-%v", i+1), string(data))
	}
}
