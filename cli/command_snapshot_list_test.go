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
}
