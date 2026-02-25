package cli_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotMigrate(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)

	sourceEnv := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)
	defer sourceEnv.RunAndExpectSuccess(t, "repo", "disconnect")

	sourceEnv.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", sourceEnv.RepoDir)

	srcdir := testutil.TempDirectory(t)
	require.NoError(t, os.WriteFile(filepath.Join(srcdir, "some-file2"), []byte{1, 2, 3}, 0o755))

	// Take a snapshot on the source repository
	sourceEnv.RunAndExpectSuccess(t, "snapshot", "create", "--tags", "one:1", "--tags", "two:2", srcdir)

	// Verify the tags were stored in the manifest
	var snapshotsSource []*cli.SnapshotManifest
	testutil.MustParseJSONLines(t, sourceEnv.RunAndExpectSuccess(t, "snapshot", "list",
		"--json"), &snapshotsSource)
	require.Len(t, snapshotsSource, 1)

	require.Equal(t, "1", snapshotsSource[0].Tags["tag:one"])
	require.Equal(t, "2", snapshotsSource[0].Tags["tag:two"])

	destEnv := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)
	defer destEnv.RunAndExpectSuccess(t, "repo", "disconnect")

	destEnv.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", destEnv.RepoDir)

	// Migrate the snapshot to the destination repository
	destEnv.RunAndExpectSuccess(
		t,
		"snapshot", "migrate",
		"--tags", "tag:one",
		"--tags", "tag:two",
		"--all",
		"--source-config", filepath.Join(sourceEnv.ConfigDir, ".kopia.config"),
	)

	// Verify the tags have been migrated to the destination repository
	var snapshotsDestination []*cli.SnapshotManifest
	testutil.MustParseJSONLines(t, destEnv.RunAndExpectSuccess(t, "snapshot", "list",
		"--json"), &snapshotsDestination)

	require.Len(t, snapshotsDestination, 1)
	require.Equal(t, "1", snapshotsDestination[0].Tags["tag:one"])
	require.Equal(t, "2", snapshotsDestination[0].Tags["tag:two"])
}
