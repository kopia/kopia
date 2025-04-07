package maintenance_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/epoch"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

// Ensure quick maintenance runs when the epoch manager is enabled.
func TestQuickMaintenanceRunWithEpochManager(t *testing.T) {
	t.Parallel()

	ctx, env := repotesting.NewEnvironment(t, format.FormatVersion3)

	// set the repository owner since it is not set by NewEnvironment
	setRepositoryOwner(t, ctx, env.RepositoryWriter)
	verifyEpochManagerIsEnabled(t, ctx, env.Repository)
	verifyEpochTasksRunsInQuickMaintenance(t, ctx, env.RepositoryWriter)
}

func TestQuickMaintenanceAdvancesEpoch(t *testing.T) {
	t.Parallel()

	ft := faketime.NewAutoAdvance(time.Date(2024, time.October, 18, 0, 0, 0, 0, time.UTC), time.Second)
	ctx, env := repotesting.NewEnvironment(t, format.FormatVersion3, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	// set the repository owner since it is not set by NewEnvironment
	setRepositoryOwner(t, ctx, env.RepositoryWriter)

	emgr, mp := verifyEpochManagerIsEnabled(t, ctx, env.Repository)

	countThreshold := mp.EpochParameters.EpochAdvanceOnCountThreshold
	epochDuration := mp.EpochParameters.MinEpochDuration

	err := env.Repository.Refresh(ctx)
	require.NoError(t, err)

	// write countThreshold index blobs: writing an object & flushing creates
	// an index blob
	for c := range countThreshold {
		err = repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) (err error) {
			ow := w.NewObjectWriter(ctx, object.WriterOptions{})
			require.NotNil(t, ow)

			defer func() {
				cerr := ow.Close()
				err = errors.Join(err, cerr)
			}()

			_, err = fmt.Fprintf(ow, "%v-%v", 0, c) // epoch count, object count
			if err != nil {
				return err
			}

			_, err = ow.Result() // force content write

			return err
		})

		require.NoError(t, err)
	}

	// advance time and write more index to force epoch advancement on maintenance
	ft.Advance(epochDuration + time.Second)
	ow := env.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{})
	require.NotNil(t, ow)

	_, err = fmt.Fprintf(ow, "%v-%v", 0, "last-object-in-epoch")
	require.NoError(t, err)

	_, err = ow.Result() // force content write
	require.NoError(t, err)

	err = ow.Close()
	require.NoError(t, err)

	// verify that there are enough index blobs to advance the epoch
	epochSnap, err := emgr.Current(ctx)
	require.NoError(t, err)

	err = env.RepositoryWriter.Flush(ctx)
	require.NoError(t, err)

	require.Zero(t, epochSnap.WriteEpoch, "write epoch was advanced")
	require.GreaterOrEqual(t, len(epochSnap.UncompactedEpochSets[0]), countThreshold, "not enough index blobs were written")

	verifyEpochTasksRunsInQuickMaintenance(t, ctx, env.RepositoryWriter)

	// verify epoch was advanced
	err = emgr.Refresh(ctx)
	require.NoError(t, err)

	epochSnap, err = emgr.Current(ctx)
	require.NoError(t, err)
	require.Positive(t, epochSnap.WriteEpoch, "write epoch was NOT advanced")
}

func setRepositoryOwner(t *testing.T, ctx context.Context, rep repo.RepositoryWriter) {
	t.Helper()

	maintParams, err := maintenance.GetParams(ctx, rep)
	require.NoError(t, err)

	co := rep.ClientOptions()
	require.NotZero(t, co)

	maintParams.Owner = co.UsernameAtHost()

	err = maintenance.SetParams(ctx, rep, maintParams)
	require.NoError(t, err)

	require.NoError(t, rep.Flush(ctx))

	// verify the owner was set
	maintParams, err = maintenance.GetParams(ctx, rep)
	require.NoError(t, err)
	require.Equal(t, co.UsernameAtHost(), maintParams.Owner)
}

func verifyEpochManagerIsEnabled(t *testing.T, ctx context.Context, rep repo.Repository) (*epoch.Manager, format.MutableParameters) {
	t.Helper()

	// verify epoch manager is enabled
	dr, isDirect := rep.(repo.DirectRepository)
	require.True(t, isDirect)
	require.NotNil(t, dr)

	fm := dr.FormatManager()
	require.NotNil(t, fm)

	mp, err := fm.GetMutableParameters(ctx)
	require.NoError(t, err)
	require.True(t, mp.EpochParameters.Enabled, "epoch manager not enabled")

	emgr, enabled, err := dr.ContentReader().EpochManager(ctx)
	require.NoError(t, err)
	require.True(t, enabled, "epoch manager not enabled")

	return emgr, mp
}

func verifyEpochTasksRunsInQuickMaintenance(t *testing.T, ctx context.Context, rep repo.DirectRepositoryWriter) {
	t.Helper()

	// verify quick maintenance has NOT run yet
	sch, err := maintenance.GetSchedule(ctx, rep)

	require.NoError(t, err)
	require.True(t, sch.NextFullMaintenanceTime.IsZero(), "unexpected NextFullMaintenanceTime")
	require.True(t, sch.NextQuickMaintenanceTime.IsZero(), "unexpected NextQuickMaintenanceTime")

	err = snapshotmaintenance.Run(ctx, rep, maintenance.ModeQuick, false, maintenance.SafetyFull)
	require.NoError(t, err)

	// verify quick maintenance ran
	sch, err = maintenance.GetSchedule(ctx, rep)

	require.NoError(t, err)
	require.False(t, sch.NextQuickMaintenanceTime.IsZero(), "unexpected NextQuickMaintenanceTime")
	require.True(t, sch.NextFullMaintenanceTime.IsZero(), "unexpected NextFullMaintenanceTime")
	require.NotEmpty(t, sch.Runs, "quick maintenance did not run")

	// note: this does not work => require.Contains(t, sch.Runs, maintenance.TaskEpochAdvance)
	r, exists := sch.Runs[maintenance.TaskEpochAdvance]
	require.True(t, exists)
	require.NotEmpty(t, r)

	r, exists = sch.Runs[maintenance.TaskEpochCompactSingle]
	require.True(t, exists)
	require.NotEmpty(t, r)
}
