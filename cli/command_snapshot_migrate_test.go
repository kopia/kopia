package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotMigrateMatchTags(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)

	sourceEnv := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)
	defer sourceEnv.RunAndExpectSuccess(t, "repo", "disconnect")

	sourceEnv.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", sourceEnv.RepoDir)

	srcdir := testutil.TempDirectory(t)

	for i, tags := range [][]string{
		{"env:prod", "tier:db"},
		{"env:prod", "tier:web"},
		{"env:dev", "tier:db"},
	} {
		require.NoError(t, os.WriteFile(filepath.Join(srcdir, "file"+string(rune('a'+i))), []byte{byte(i)}, 0o755))

		args := []string{"snapshot", "create", srcdir}
		for _, tag := range tags {
			args = append(args, "--tags", tag)
		}

		sourceEnv.RunAndExpectSuccess(t, args...)
	}

	var sourceSnapshots []*cli.SnapshotManifest

	testutil.MustParseJSONLines(t, sourceEnv.RunAndExpectSuccess(t, "snapshot", "list", "--json"), &sourceSnapshots)
	require.Len(t, sourceSnapshots, 3)

	destEnv := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)
	defer destEnv.RunAndExpectSuccess(t, "repo", "disconnect")

	destEnv.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", destEnv.RepoDir)

	// only the first snapshot has both tags
	destEnv.RunAndExpectSuccess(
		t,
		"snapshot", "migrate",
		"--all",
		"--match-tags", "env:prod",
		"--match-tags", "tier:db",
		"--source-config", filepath.Join(sourceEnv.ConfigDir, ".kopia.config"),
	)

	var destSnapshots []*cli.SnapshotManifest

	testutil.MustParseJSONLines(t, destEnv.RunAndExpectSuccess(t, "snapshot", "list", "--json"), &destSnapshots)
	require.Len(t, destSnapshots, 1)
	// object IDs differ across repositories, compare the directory contents instead
	require.EqualValues(t, 1, sourceSnapshots[0].RootEntry.DirSummary.TotalFileCount)
	require.Equal(t, sourceSnapshots[0].RootEntry.DirSummary.TotalFileCount, destSnapshots[0].RootEntry.DirSummary.TotalFileCount)

	latestEnv := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)
	defer latestEnv.RunAndExpectSuccess(t, "repo", "disconnect")

	latestEnv.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", latestEnv.RepoDir)

	// the latest snapshot is env:dev, so --latest-only picks the second one
	latestEnv.RunAndExpectSuccess(
		t,
		"snapshot", "migrate",
		"--all",
		"--latest-only",
		"--match-tags", "env:prod",
		"--source-config", filepath.Join(sourceEnv.ConfigDir, ".kopia.config"),
	)

	var latestSnapshots []*cli.SnapshotManifest

	testutil.MustParseJSONLines(t, latestEnv.RunAndExpectSuccess(t, "snapshot", "list", "--json"), &latestSnapshots)
	require.Len(t, latestSnapshots, 1)
	require.EqualValues(t, 2, sourceSnapshots[1].RootEntry.DirSummary.TotalFileCount)
	require.Equal(t, sourceSnapshots[1].RootEntry.DirSummary.TotalFileCount, latestSnapshots[0].RootEntry.DirSummary.TotalFileCount)
}

func TestSnapshotMigrateMatchTagsInvalid(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)

	sourceEnv := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)
	defer sourceEnv.RunAndExpectSuccess(t, "repo", "disconnect")

	sourceEnv.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", sourceEnv.RepoDir)

	srcdir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(srcdir, "file"), []byte{1}, 0o755))

	sourceEnv.RunAndExpectSuccess(t, "snapshot", "create", "--tags", "env:prod", srcdir)

	destEnv := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)
	defer destEnv.RunAndExpectSuccess(t, "repo", "disconnect")

	destEnv.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", destEnv.RepoDir)

	_, stderr := destEnv.RunAndExpectFailure(
		t,
		"snapshot", "migrate",
		"--all",
		"--match-tags", "env",
		"--source-config", filepath.Join(sourceEnv.ConfigDir, ".kopia.config"),
	)
	require.Contains(t, strings.Join(stderr, "\n"), "invalid --match-tags")

	// nothing is copied before the value is rejected
	var destSnapshots []*cli.SnapshotManifest

	testutil.MustParseJSONLines(t, destEnv.RunAndExpectSuccess(t, "snapshot", "list", "--json"), &destSnapshots)
	require.Empty(t, destSnapshots)
}
