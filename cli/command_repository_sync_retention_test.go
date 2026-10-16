package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/tests/testenv"
)

const testRetentionPeriod = 24 * time.Hour

// listBlobRetention returns the retention info for all blobs in the given
// storage, separated into locking-prefixed and non-locking blob IDs.
func listBlobRetention(t *testing.T, st blobtesting.RetentionStorage) (locking, nonLocking map[blob.ID]time.Time) {
	t.Helper()

	ctx := testlogging.Context(t)

	locking = map[blob.ID]time.Time{}
	nonLocking = map[blob.ID]time.Time{}

	require.NoError(t, st.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		mode, retainUntil, err := st.GetRetention(ctx, bm.BlobID)
		require.NoError(t, err, "getting retention for %v", bm.BlobID)

		if repo.IsLockingStorageBlobID(bm.BlobID) {
			require.Equal(t, blob.Governance, mode, "unexpected retention mode on %v", bm.BlobID)
			locking[bm.BlobID] = retainUntil
		} else {
			require.Empty(t, mode, "unexpected retention mode on non-locking blob %v", bm.BlobID)
			nonLocking[bm.BlobID] = retainUntil
		}

		return nil
	}))

	return locking, nonLocking
}

func TestRepositorySyncRetention(t *testing.T) {
	startTime := clock.Now()
	ft := faketime.NewTimeAdvance(startTime)

	runner := testenv.NewInProcRunner(t)
	env := testenv.NewCLITest(t, []string{}, runner)

	// source repository in reconnectable in-memory storage driven by the same
	// fake clock as the destination, so blob timestamps are deterministic and
	// unchanged blobs are classified as in-sync on subsequent runs.
	srcSt := repotesting.NewReconnectableStorage(t, blobtesting.NewVersionedMapStorage(ft.NowFunc()))
	srcOpt := testutil.EnsureType[*repotesting.ReconnectableStorageOptions](t, srcSt.ConnectionInfo().Config)

	dstFake := blobtesting.NewVersionedMapStorage(ft.NowFunc())
	dstSt := repotesting.NewReconnectableStorage(t, dstFake)
	dstOpt := testutil.EnsureType[*repotesting.ReconnectableStorageOptions](t, dstSt.ConnectionInfo().Config)

	env.RunAndExpectSuccess(t, "repo", "create", "in-memory", "--uuid", srcOpt.UUID)

	srcDir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("hello world"), 0o600))
	env.RunAndExpectSuccess(t, "snapshot", "create", srcDir)

	// invalid flag combinations
	env.RunAndExpectFailure(t, "repo", "sync-to", "in-memory", "--uuid", dstOpt.UUID,
		"--retention-mode", blob.Governance.String())
	env.RunAndExpectFailure(t, "repo", "sync-to", "in-memory", "--uuid", dstOpt.UUID,
		"--retention-mode", blob.Governance.String(), "--retention-period", "1h")
	env.RunAndExpectFailure(t, "repo", "sync-to", "in-memory", "--uuid", dstOpt.UUID,
		"--retention-mode", "invalid-mode", "--retention-period", "24h")

	// initial sync with retention applies object locks to locking-prefixed
	// blobs, including the initial kopia.repository write.
	env.RunAndExpectSuccess(t, "repo", "sync-to", "in-memory", "--uuid", dstOpt.UUID,
		"--retention-mode", blob.Governance.String(), "--retention-period", "24h")

	locking, nonLocking := listBlobRetention(t, dstFake)
	require.NotEmpty(t, locking)
	require.Contains(t, locking, blob.ID("kopia.repository"))

	for id, retainUntil := range locking {
		require.Equal(t, startTime.Add(testRetentionPeriod), retainUntil, "unexpected retention time on %v", id)
	}

	for id, retainUntil := range nonLocking {
		require.True(t, retainUntil.IsZero(), "unexpected retention on non-locking blob %v", id)
	}

	// re-running the sync later extends the object locks on in-sync blobs.
	extendTime := ft.Advance(12 * time.Hour)

	env.RunAndExpectSuccess(t, "repo", "sync-to", "in-memory", "--uuid", dstOpt.UUID,
		"--retention-mode", blob.Governance.String(), "--retention-period", "24h")

	locking, _ = listBlobRetention(t, dstFake)
	require.NotEmpty(t, locking)

	for id, retainUntil := range locking {
		require.Equal(t, extendTime.Add(testRetentionPeriod), retainUntil, "lock not extended on %v", id)
	}

	// --no-extend-object-locks skips the extension phase.
	ft.Advance(1 * time.Hour)
	env.RunAndExpectSuccess(t, "repo", "sync-to", "in-memory", "--uuid", dstOpt.UUID,
		"--retention-mode", blob.Governance.String(), "--retention-period", "24h",
		"--no-extend-object-locks")

	locking, _ = listBlobRetention(t, dstFake)
	for id, retainUntil := range locking {
		require.Equal(t, extendTime.Add(testRetentionPeriod), retainUntil, "lock unexpectedly extended on %v", id)
	}

	// --dry-run reports the extension candidates but does not extend.
	_, stderr := env.RunAndExpectSuccessWithErrOut(t, "repo", "sync-to", "in-memory", "--uuid", dstOpt.UUID,
		"--retention-mode", blob.Governance.String(), "--retention-period", "24h",
		"--dry-run")
	require.Contains(t, strings.Join(stderr, "\n"), "BLOBs to extend object locks on")

	locking, _ = listBlobRetention(t, dstFake)
	for id, retainUntil := range locking {
		require.Equal(t, extendTime.Add(testRetentionPeriod), retainUntil, "lock unexpectedly extended on %v during dry-run", id)
	}

	// the synchronized copy is a fully working repository.
	env.RunAndExpectSuccess(t, "repo", "disconnect")
	env.RunAndExpectSuccess(t, "repo", "connect", "in-memory", "--uuid", dstOpt.UUID)
	env.RunAndExpectSuccess(t, "snapshot", "list")
	env.RunAndExpectSuccess(t, "repo", "disconnect")
}

func TestRepositorySyncRetentionUnsupportedDestination(t *testing.T) {
	runner := testenv.NewInProcRunner(t)
	env := testenv.NewCLITest(t, []string{}, runner)

	env.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", env.RepoDir)

	srcDir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("hello world"), 0o600))
	env.RunAndExpectSuccess(t, "snapshot", "create", srcDir)

	// fresh destination: fails on the initial format blob write.
	dstDir := testutil.TempDirectory(t)
	_, stderr := env.RunAndExpectFailure(t, "repo", "sync-to", "filesystem", "--path", dstDir,
		"--retention-mode", blob.Governance.String(), "--retention-period", "24h")
	require.Contains(t, strings.Join(stderr, "\n"), "does not support object-lock retention")

	// destination synced without retention, then an extension-only run also
	// fails with the same actionable message.
	env.RunAndExpectSuccess(t, "repo", "sync-to", "filesystem", "--path", dstDir)
	_, stderr = env.RunAndExpectFailure(t, "repo", "sync-to", "filesystem", "--path", dstDir,
		"--retention-mode", blob.Governance.String(), "--retention-period", "24h")
	require.Contains(t, strings.Join(stderr, "\n"), "does not support object-lock retention")
}
