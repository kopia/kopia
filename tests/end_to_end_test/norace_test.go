//go:build !race
// +build !race

package endtoend_test

import (
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testenv"
)

// Exclude tests below from the -race detection test, because they are resource
// intensive and way too slow.

func TestSnapshotNoLeftoverCheckpoints(t *testing.T) {
	// 1 GiB of data seems to be enough for the snapshot time to exceed one second.
	const (
		fileSize                  = int64(1) << 30
		checkpointInterval        = "1s"
		checkpointIntervalSeconds = float64(1)
	)

	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	baseDir := t.TempDir()
	writeRandomFile(t, filepath.Join(baseDir, "foo"), fileSize)

	startTime := clock.Now()

	e.RunAndExpectSuccess(t, "snapshot", "create", baseDir, "--checkpoint-interval", checkpointInterval)

	require.Greater(t, clock.Now().Sub(startTime).Seconds(), checkpointIntervalSeconds)

	// This exploits the implementation detail of `ListSnapshotsAndExpectSuccess`, that it does
	// not sanitize `targets` to exclude flags.
	si := clitestutil.ListSnapshotsAndExpectSuccess(t, e, "--incomplete", baseDir)
	require.Len(t, si, 1)
	require.Len(t, si[0].Snapshots, 1)
	require.False(t, si[0].Snapshots[0].Incomplete)
}

func writeRandomFile(t *testing.T, name string, fileSize int64) {
	t.Helper()

	f, err := os.Create(name)

	require.NoError(t, err)
	require.NotNil(t, f)

	defer func() {
		require.NoError(t, f.Close())
	}()

	n, err := io.CopyN(f, rand.New(rand.NewSource(0)), fileSize)
	require.NoError(t, err)
	require.Equal(t, fileSize, n)
}
