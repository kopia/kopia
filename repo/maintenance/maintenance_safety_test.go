package maintenance_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

func (s *formatSpecificTestSuite) TestMaintenanceSafety(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TraceStorage = true
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	anotherClient := env.MustConnectOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ft.NowFunc()
	})

	var objectID object.ID

	// create object that's immediately orphaned since nobody refers to it.
	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y", MetadataCompressor: "zstd-fastest"})
		fmt.Fprintf(ow, "hello world")
		var err error
		objectID, err = ow.Result()
		return err
	}))

	// create another object in separate pack.
	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y", MetadataCompressor: "zstd-fastest"})
		fmt.Fprintf(ow, "hello universe")
		_, err := ow.Result()
		return err
	}))

	// both 'main' and 'another' client can see it
	t.Logf("**** MAINTENANCE #1")
	require.NoError(t, anotherClient.Refresh(ctx))
	verifyContentDeletedState(ctx, t, env.Repository, objectID, false)
	verifyObjectReadable(ctx, t, env.Repository, objectID)
	verifyObjectReadable(ctx, t, anotherClient, objectID)

	// maintenance has no effect since there was no previous GC
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	verifyContentDeletedState(ctx, t, env.Repository, objectID, false)

	t.Logf("**** MAINTENANCE #2")

	ft.Advance(25 * time.Hour)
	// at this point there was a previous GC so content gets marked as deleted but is still readable.
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	verifyContentDeletedState(ctx, t, env.Repository, objectID, true)

	verifyObjectReadable(ctx, t, env.Repository, objectID)
	verifyObjectReadable(ctx, t, anotherClient, objectID)

	t.Logf("**** MAINTENANCE #3")
	ft.Advance(4 * time.Hour)

	// run maintenance again - this time we'll rewrite the two objects together.
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// the object is still readable using main client because it has updated indexes after
	// rewrite.
	require.NoError(t, env.Repository.Refresh(ctx))
	verifyObjectReadable(ctx, t, env.Repository, objectID)

	// verify that object is still readable using another client, to ensure we did not
	// immediately delete the blob that was rewritten.
	verifyObjectReadable(ctx, t, anotherClient, objectID)

	t.Logf("**** MAINTENANCE #4")
	ft.Advance(4 * time.Hour)
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	verifyObjectReadable(ctx, t, anotherClient, objectID)
	verifyObjectReadable(ctx, t, env.Repository, objectID)

	t.Logf("**** MAINTENANCE #5")
	ft.Advance(4 * time.Hour)
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	verifyObjectNotFound(ctx, t, env.Repository, objectID)
	verifyObjectNotFound(ctx, t, anotherClient, objectID)
}

func verifyContentDeletedState(ctx context.Context, t *testing.T, rep repo.Repository, objectID object.ID, want bool) {
	t.Helper()

	cid, _, _ := objectID.ContentID()

	info, err := rep.ContentInfo(ctx, cid)
	require.NoError(t, err)
	require.Equal(t, want, info.Deleted)
}

func verifyObjectReadable(ctx context.Context, t *testing.T, rep repo.Repository, objectID object.ID) {
	t.Helper()

	require.NoError(t, repo.WriteSession(ctx, rep, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		r, err := w.OpenObject(ctx, objectID)
		require.NoError(t, err)
		r.Close()
		return nil
	}))
}

func verifyObjectNotFound(ctx context.Context, t *testing.T, rep repo.Repository, objectID object.ID) {
	t.Helper()

	require.NoError(t, repo.WriteSession(ctx, rep, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		_, err := w.OpenObject(ctx, objectID)
		require.ErrorIs(t, err, object.ErrObjectNotFound)
		return nil
	}))
}
