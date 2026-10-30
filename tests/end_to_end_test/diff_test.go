package endtoend_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestDiff(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	dataDir := testutil.TempDirectory(t)

	// initial snapshot
	require.NoError(t, os.MkdirAll(dataDir, 0o777))
	e.RunAndExpectSuccess(t, "snapshot", "create", dataDir)

	// create some directories and files
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "foo"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "some-file1"), []byte(`
hello world
how are you
`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "some-file2"), []byte(`
quick brown
fox jumps
over the lazy
dog
`), 0o600))
	e.RunAndExpectSuccess(t, "snapshot", "create", dataDir)

	// change some files
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "some-file2"), []byte(`
quick brown
fox jumps
over the lazy
canary
`), 0o600))

	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "bar"), 0o700))
	e.RunAndExpectSuccess(t, "snapshot", "create", dataDir)

	// change some files
	os.Remove(filepath.Join(dataDir, "some-file1"))

	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "bar"), 0o700))
	e.RunAndExpectSuccess(t, "snapshot", "create", dataDir)

	si := clitestutil.ListSnapshotsAndExpectSuccess(t, e, dataDir)
	if got, want := len(si), 1; got != want {
		t.Fatalf("got %v sources, wanted %v", got, want)
	}

	// make sure we can generate between all versions of the directory
	snapshots := si[0].Snapshots
	for _, s1 := range snapshots {
		for _, s2 := range snapshots {
			e.RunAndExpectSuccess(t, "diff", "-f", s1.ObjectID, s2.ObjectID)
		}
	}
}
