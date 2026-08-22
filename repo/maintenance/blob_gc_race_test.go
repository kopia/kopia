package maintenance_test

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/object"
)

// Helper function to list blobs with a given prefix
func listBlobsWithPrefix(t *testing.T, ctx context.Context, br blob.Reader, prefix blob.ID) map[blob.ID]blob.Metadata {
	t.Helper()
	blobs := make(map[blob.ID]blob.Metadata)
	err := br.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
		blobs[bm.BlobID] = bm
		return nil
	})
	require.NoError(t, err)
	return blobs
}

// Helper function to list all pack blobs (p and q prefixes)
func listPackBlobs(t *testing.T, ctx context.Context, br blob.Reader) map[blob.ID]blob.Metadata {
	t.Helper()
	packBlobs := make(map[blob.ID]blob.Metadata)

	maps.Copy(packBlobs, listBlobsWithPrefix(t, ctx, br, blob.ID("p")))
	maps.Copy(packBlobs, listBlobsWithPrefix(t, ctx, br, blob.ID("q")))

	return packBlobs
}

// Helper function to list all index blobs (n and m prefixes)
func listIndexBlobs(t *testing.T, ctx context.Context, br blob.Reader) map[blob.ID]blob.Metadata {
	t.Helper()
	indexBlobs := make(map[blob.ID]blob.Metadata)

	maps.Copy(indexBlobs, listBlobsWithPrefix(t, ctx, br, blob.ID("n")))
	maps.Copy(indexBlobs, listBlobsWithPrefix(t, ctx, br, blob.ID("xn")))

	return indexBlobs
}

// Helper function to assert blob count
func assertBlobCount(t *testing.T, blobs map[blob.ID]blob.Metadata, expectedCount int, description string) {
	t.Helper()
	actualCount := len(blobs)
	require.Equal(t, expectedCount, actualCount, "%s: expected %d blobs, got %d", description, expectedCount, actualCount)
	t.Logf("%s: Found %d blobs", description, actualCount)
}

// TestDeleteUnreferencedBlobs_DelayedFlushRace reproduces the precise sequence that leads to data corruption:
//
// Timeline:
// T0 (T+0min): Writer flushes to create initial index blobs
// T1 (T+1min): Writer writes 30MB content causing pack blobs to be written, but NO Flush() yet
// T2 (T+25h30min): Maintenance starts (separate process), captures cutoff time
// T3 (T+25h30min10s): Writer finally calls Flush(), writes index blobs
// T4 (T+25h30min10s): Maintenance runs GC with stale index view, deletes valid pack blobs
// T5 (T+25h31min10s): New reader process loads indexes and finds corrupted content
func (s *formatSpecificTestSuite) TestDeleteUnreferencedBlobs_DelayedFlushRace(t *testing.T) {
	ta := faketime.NewClockTimeWithOffset(0)

	// Create shared storage that will be used by multiple repository instances
	ctx, writerEnv := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ta.NowFunc()
		},
	})
	defer writerEnv.Close(ctx, t)

	// === T0: Writer flushes to create initial index blobs ===

	// Verify we start with zero pack blobs
	initialPackBlobs := listPackBlobs(t, ctx, writerEnv.RepositoryWriter.BlobReader())
	assertBlobCount(t, initialPackBlobs, 0, "Initial pack blobs")

	// Flush to create index blobs
	require.NoError(t, writerEnv.RepositoryWriter.Flush(ctx))

	// === T1: T+1min - Writer creates content but doesn't flush ===
	ta.Advance(1 * time.Minute)

	// Write enough content to trigger pack creation (30MB)
	var contentIDs []object.ID
	for i := range 100 { // Write many objects to fill a pack
		w := writerEnv.RepositoryWriter.NewObjectWriter(ctx, object.WriterOptions{})
		// Write ~300KB per object to reach ~30MB total
		largeContent := make([]byte, 300*1024)
		for j := range largeContent {
			largeContent[j] = byte(i + j) // Some variation in content
		}
		w.Write(largeContent)

		objID, err := w.Result()
		require.NoError(t, err)
		contentIDs = append(contentIDs, objID)
	}

	// make sure some pack blobs were created but no index blobs
	require.NotEmpty(t, listPackBlobs(t, ctx, writerEnv.RepositoryWriter.BlobReader()))
	require.Empty(t, listIndexBlobs(t, ctx, writerEnv.RepositoryWriter.BlobReader()))

	// === Advance time for maintenance to start ===
	ta.Advance(24*time.Hour + 29*time.Minute) // Advance to T+25h30min

	// === T2: T+25h30min - Maintenance starts (separate process) ===
	maintenanceStartTime := ta.NowFunc()()

	// Create separate maintenance environment sharing the same storage
	maintenanceRepo := writerEnv.MustOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ta.NowFunc()
	})
	defer maintenanceRepo.Close(ctx)

	// Get DirectRepositoryWriter for maintenance operations
	directRepo := testutil.EnsureType[repo.DirectRepository](t, maintenanceRepo)
	_, maintenanceWriter, err := directRepo.NewDirectWriter(ctx, repo.WriteSessionOptions{Purpose: "maintenance"})
	require.NoError(t, err)
	defer maintenanceWriter.Close(ctx)

	// === T3: T+25h30min10s - Writer finally calls Flush() ===
	ta.Advance(10 * time.Second)
	require.NoError(t, writerEnv.RepositoryWriter.Flush(ctx))

	// === Verify index blobs were created after Flush() ===
	require.NotEmpty(t, listIndexBlobs(t, ctx, writerEnv.RepositoryWriter.BlobReader()))

	// === T4: T+25h30min10s - Maintenance runs GC with stale index view ===
	opts := maintenance.DeleteUnreferencedBlobsOptions{
		Parallel: 1,
		DryRun:   false,
		// This is the key: cutoff time from when maintenance started (T1)
		NotAfterTime: maintenanceStartTime,
	}

	// Disable index refresh during GC to simulate stale index view
	maintenanceWriter.DisableIndexRefresh()

	deletedCount, err := maintenance.DeleteUnreferencedBlobs(ctx, maintenanceWriter, opts, maintenance.SafetyFull)
	require.NoError(t, err)
	assert.Zero(t, deletedCount)

	// reader process now fails to open the content because the pack blob was deleted
	readerRepo := writerEnv.MustOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ta.NowFunc()
	})
	defer readerRepo.Close(ctx)

	for _, objID := range contentIDs {
		_, err := readerRepo.OpenObject(ctx, objID)
		require.NoError(t, err)
	}
}
