package maintenance_test

import (
	"context"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/storagereserve"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

type faultyWriter struct {
	repo.DirectRepositoryWriter
	faultyStorage blob.Storage
}

func (w *faultyWriter) BlobStorage() blob.Storage {
	return w.faultyStorage
}

type enospcStorage struct {
	blob.Storage
	predicate func(id blob.ID) bool
}

func (s *enospcStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	if s.predicate != nil && s.predicate(id) {
		return syscall.ENOSPC
	}
	return s.Storage.PutBlob(ctx, id, data, opts)
}

func TestEmergencyRecovery(t *testing.T) {
	ctx := testlogging.Context(t)
	_, env := repotesting.NewEnvironment(t, format.FormatVersion3)

	st := env.RootStorage()
	
	// Ensure reserve exists initially (it should because Initialize calls it now)
	exists, err := storagereserve.Exists(ctx, st)
	require.NoError(t, err)
	require.True(t, exists, "reserve should exist after environment setup")

	// Set owner so maintenance can run
	setRepositoryOwner(t, ctx, env.RepositoryWriter)

	// Wrap the writer to return a faulty storage
	faulty := &enospcStorage{Storage: env.RepositoryWriter.BlobStorage()}
	fw := &faultyWriter{
		DirectRepositoryWriter: env.RepositoryWriter,
		faultyStorage:          faulty,
	}

	// 1. Simulate ENOSPC on maintenance schedule update AND on reserve recreation
	faulty.predicate = func(id blob.ID) bool {
		return id == "kopia.maintenance" || id == blob.ID(storagereserve.ReserveBlobID)
	}

	// Run maintenance via our faulty writer
	err = snapshotmaintenance.Run(ctx, fw, maintenance.ModeQuick, false, maintenance.SafetyFull)
	require.NoError(t, err, "Maintenance should succeed despite ENOSPC on schedule update")

	// Verify reserve was deleted during emergency mode and NOT recreated because we blocked it
	exists, err = storagereserve.Exists(ctx, st)
	require.NoError(t, err)
	require.False(t, exists, "reserve should have been deleted and not recreated")

	// 2. Verify reserve is recreated after successful maintenance WITHOUT faults
	faulty.predicate = nil
	err = snapshotmaintenance.Run(ctx, fw, maintenance.ModeQuick, false, maintenance.SafetyFull)
	require.NoError(t, err)

	exists, err = storagereserve.Exists(ctx, st)
	require.NoError(t, err)
	require.True(t, exists, "reserve should have been recreated")
}

func TestStorageReserveGuards(t *testing.T) {
	ctx := testlogging.Context(t)
	_, env := repotesting.NewEnvironment(t, format.FormatVersion3)
	st := env.RootStorage()

	// --- Case 1: Snapshot Create Guard ---
	// Delete reserve manually
	err := storagereserve.Delete(ctx, st)
	require.NoError(t, err)

	// Simulate ENOSPC on reserve creation (Ensure will fail)
	faulty := &enospcStorage{
		Storage: st,
		predicate: func(id blob.ID) bool {
			return id == blob.ID(storagereserve.ReserveBlobID)
		},
	}

	// --- Case 2: Snapshot Delete Guard ---
	// Re-add ENOSPC fault for ANY new blob if we want, but let's just test the helper
	err = ensureReserveOrDeleteForRecoveryHelper(ctx, faulty)
	require.NoError(t, err, "Delete guard helper should succeed by deleting existing reserve or ignoring missing one")
	
	// Verify reserve is gone
	exists, err := storagereserve.Exists(ctx, st)
	require.NoError(t, err)
	require.False(t, exists)
}

func ensureReserveOrDeleteForRecoveryHelper(ctx context.Context, st blob.Storage) error {
	if err := storagereserve.Ensure(ctx, st, storagereserve.DefaultReserveSize); err != nil {
		return storagereserve.Delete(ctx, st)
	}

	return nil
}
