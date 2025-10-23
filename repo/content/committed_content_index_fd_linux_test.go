//go:build linux

package content

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/repodiag"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
)

// countFDsLinux returns the number of open file descriptors for the current process on Linux.
func countFDsLinux(t *testing.T) int {
	t.Helper()

	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err, "unable to read /proc/self/fd")

	return len(entries)
}

// Test that opening many indexes on Linux does not retain a file descriptor per index.
func TestCommittedContentIndexCache_Disk_FDsNotGrowingOnOpen_Linux(t *testing.T) {
	// Do not run in parallel to avoid fd count noise from other tests.
	var lm *repodiag.LogManager

	ctx := testlogging.Context(t)
	ft := faketime.NewClockTimeWithOffset(0)
	cache := &diskCommittedContentIndexCache{
		testutil.TempDirectory(t),
		ft.NowFunc(),
		func() int { return 3 },
		lm.NewLogger("test"),
		DefaultIndexCacheSweepAge,
	}

	const indexCount = 200

	// Prepare N small indexes in the cache directory.
	for i := range indexCount {
		b := index.Builder{
			mustParseID(t, fmt.Sprintf("c%03d", i)): Info{PackBlobID: blob.ID(fmt.Sprintf("p%03d", i)), ContentID: mustParseID(t, fmt.Sprintf("c%03d", i))},
		}
		require.NoError(t, cache.addContentToCache(ctx, blob.ID(fmt.Sprintf("ndx%03d", i)), mustBuildIndex(t, b)))
	}

	before := countFDsLinux(t)

	var opened []index.Index

	// Open all indexes and keep them open to maximize pressure.
	for i := range indexCount {
		ndx, err := cache.openIndex(ctx, blob.ID(fmt.Sprintf("ndx%03d", i)))
		require.NoError(t, err)

		opened = append(opened, ndx)
	}

	after := countFDsLinux(t)

	// Despite keeping many mappings alive, the FD count should not grow proportionally.
	// Allow some slack for incidental FDs opened by runtime or test harness.
	const maxDelta = 32

	require.LessOrEqualf(t, after-before, maxDelta, "fd count grew too much after opening %d indexes", indexCount)

	// Cleanup
	for _, ndx := range opened {
		require.NoError(t, ndx.Close())
	}
}
