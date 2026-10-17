package cli_test

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotList(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	srcdir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(srcdir, "some-file2"), []byte{1, 2, 3}, 0o755))

	var man cli.SnapshotManifest

	e.RunAndExpectSuccess(t, "policy", "set", srcdir, "--keep-latest=4", "--keep-hourly=0", "--keep-daily=0", "--keep-monthly=0", "--keep-weekly=0", "--keep-annual=0")

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", srcdir, "--json"), &man)

	require.NoError(t, os.WriteFile(filepath.Join(srcdir, "some-file3"), []byte{1, 2, 3, 4}, 0o755))
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", srcdir, "--json"), &man)

	require.NoError(t, os.WriteFile(filepath.Join(srcdir, "some-file4"), []byte{4}, 0o755))
	e.RunAndExpectSuccess(t, "snapshot", "create", srcdir)
	e.RunAndExpectSuccess(t, "snapshot", "create", srcdir)

	var snapshots []*cli.SnapshotManifest

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "list",
		"--json"), &snapshots)

	require.Len(t, snapshots, 4)

	for _, s := range snapshots {
		require.NotEmpty(t, s.RetentionReasons, "expecting retention reason to be set")
	}

	lines := e.RunAndExpectSuccess(t, "snapshot", "list")
	require.Len(t, lines, 5)

	require.Contains(t, lines[1], " 3 B ")
	require.Contains(t, lines[1], " files:1 dirs:1 ")

	require.Contains(t, lines[2], " 7 B ")
	require.Contains(t, lines[2], " files:2 dirs:1 ")

	require.Contains(t, lines[3], " 8 B ")
	require.Contains(t, lines[3], " files:3 dirs:1 ")

	require.Contains(t, lines[4], "+ 1 identical snapshots until")

	lines = e.RunAndExpectSuccess(t, "snapshot", "list", "-l")
	require.Len(t, lines, 5)

	require.Contains(t, lines[1], " 3 B ")
	require.Contains(t, lines[1], " files:1 dirs:1 ")

	require.Contains(t, lines[2], " 7 B ")
	require.Contains(t, lines[2], " files:2 dirs:1 ")

	require.Contains(t, lines[3], " 8 B ")
	require.Contains(t, lines[3], " files:3 dirs:1 ")

	// identical snapshot is not coalesced
	require.Contains(t, lines[4], " 8 B ")
	require.Contains(t, lines[4], " files:3 dirs:1 ")
}

// Regression test for #5326: `snapshot list --json` ignored the incomplete
// filter and always emitted incomplete snapshots, while the text output
// correctly hid them unless --incomplete/-i was passed.
func TestSnapshotListIncompleteJSON(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	srcdir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(srcdir, "small.txt"), []byte{1, 2, 3}, 0o600))

	// one complete snapshot
	e.RunAndExpectSuccess(t, "snapshot", "create", srcdir)

	// force an incomplete (checkpoint) snapshot by capping the upload below the
	// source size; the data is random so it neither compresses nor dedupes and
	// reliably exceeds the 1 MB limit.
	big := make([]byte, 5<<20)
	_, err := rand.Read(big)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(srcdir, "big.bin"), big, 0o600))
	e.RunAndExpectSuccess(t, "snapshot", "create", srcdir, "--upload-limit-mb=1")

	// sanity: with --incomplete the incomplete snapshot is present, so the
	// default-listing assertion below is not vacuously true.
	var withIncomplete []*cli.SnapshotManifest
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "list", "--json", "--incomplete"), &withIncomplete)

	var incompleteCount int

	for _, s := range withIncomplete {
		if s.IncompleteReason != "" {
			incompleteCount++
		}
	}

	require.Equal(t, 1, incompleteCount, "expected exactly one incomplete snapshot when --incomplete is set")

	// the bug: without --incomplete, --json must not list incomplete snapshots.
	var defaultList []*cli.SnapshotManifest
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "list", "--json"), &defaultList)

	for _, s := range defaultList {
		require.Empty(t, s.IncompleteReason, "snapshot list --json must not include incomplete snapshots unless --incomplete is set")
	}
}

func TestSnapshotListWithSameFileInMultipleSnapshots(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	srcdir := testutil.TempDirectory(t)

	require.NoError(t, os.MkdirAll(filepath.Join(srcdir, "a", "b", "c", "d"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srcdir, "a", "b", "c", "d", "e.txt"), []byte{1, 2, 3}, 0o755))

	e.RunAndExpectSuccess(t, "snapshot", "create", srcdir)
	e.RunAndExpectSuccess(t, "snapshot", "create", filepath.Join(srcdir, "a"))
	e.RunAndExpectSuccess(t, "snapshot", "create", filepath.Join(srcdir, "a", "b"))
	e.RunAndExpectSuccess(t, "snapshot", "create", filepath.Join(srcdir, "a", "b", "c"))
	e.RunAndExpectSuccess(t, "snapshot", "create", filepath.Join(srcdir, "a", "b", "c", "d"))
	e.RunAndExpectSuccess(t, "snapshot", "create", filepath.Join(srcdir, "a", "b", "c", "d", "e.txt"))

	var snapshots []*cli.SnapshotManifest

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "list",
		filepath.Join(srcdir, "a", "b", "c", "d", "e.txt"), "--json"), &snapshots)

	require.Len(t, snapshots, 6)

	var sps []string

	for _, s := range snapshots {
		sps = append(sps, s.Source.Path)
	}

	require.Equal(t, []string{
		srcdir,
		filepath.Join(srcdir, "a"),
		filepath.Join(srcdir, "a", "b"),
		filepath.Join(srcdir, "a", "b", "c"),
		filepath.Join(srcdir, "a", "b", "c", "d"),
		filepath.Join(srcdir, "a", "b", "c", "d", "e.txt"),
	}, sps)
}
