package endtoend_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/testenv"
)

func (s *formatSpecificTestSuite) TestSnapshotMigrate(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, s.formatFlags, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--compression=pgzip")
	e.RunAndExpectSuccess(t, "snapshot", "create", ".")

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "policy", "set", sharedTestDataDir1, "--keep-daily=77")

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir3)
	e.RunAndExpectSuccess(t, "policy", "set", sharedTestDataDir3, "--keep-daily=88")

	compressibleDir := testutil.TempDirectory(t)

	for range 10 {
		require.NoError(t, writeCompressibleFile(filepath.Join(compressibleDir, uuid.NewString())))
	}

	e.RunAndExpectSuccess(t, "snapshot", "create", compressibleDir)

	dirSize1 := testutil.MustGetTotalDirSize(t, e.RepoDir)

	sourceSnapshotCount := len(e.RunAndExpectSuccess(t, "snapshot", "list", ".", "-a"))
	sourcePolicyCount := len(e.RunAndExpectSuccess(t, "policy", "list"))

	dstenv := testenv.NewCLITest(t, s.formatFlags, runner)

	dstenv.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", dstenv.RepoDir)

	dstenv.RunAndExpectSuccess(t, "snapshot", "migrate", "--source-config", filepath.Join(e.ConfigDir, ".kopia.config"), "--all", "--parallel=5", "--overwrite-policies")
	dstenv.RunAndVerifyOutputLineCount(t, sourceSnapshotCount, "snapshot", "list", ".", "-a")
	dstenv.RunAndVerifyOutputLineCount(t, sourcePolicyCount, "policy", "list")

	// migrate again, which should be a no-op, and should not create any more policies/snapshots
	dstenv.RunAndExpectSuccess(t, "snapshot", "migrate", "--source-config", filepath.Join(e.ConfigDir, ".kopia.config"), "--all", "--overwrite-policies")
	dstenv.RunAndVerifyOutputLineCount(t, sourceSnapshotCount, "snapshot", "list", ".", "-a")
	dstenv.RunAndVerifyOutputLineCount(t, sourcePolicyCount, "policy", "list")

	// make sure compression was applied during migration
	dirSize2 := testutil.MustGetTotalDirSize(t, dstenv.RepoDir)

	require.Less(t, dirSize2, dirSize1*110/100)
}

func (s *formatSpecificTestSuite) TestSnapshotMigrateWithIgnores(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, s.formatFlags, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	sd := testutil.TempDirectory(t)

	require.NoError(t, os.WriteFile(filepath.Join(sd, "file1.txt"), []byte{1, 2, 3}, 0o666))
	require.NoError(t, os.WriteFile(filepath.Join(sd, "file2.txt"), []byte{1, 2, 3}, 0o666))

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "snapshot", "create", sd)

	dstenv := testenv.NewCLITest(t, s.formatFlags, runner)
	dstenv.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", dstenv.RepoDir)

	// now set policy to ignore file2.txt and migrate
	dstenv.RunAndExpectSuccess(t, "policy", "set", sd, "--add-ignore", "file2.txt")
	dstenv.RunAndExpectSuccess(t, "snapshot", "migrate", "--source-config", filepath.Join(e.ConfigDir, ".kopia.config"), "--all", "--apply-ignore-rules")

	var manifests []cli.SnapshotManifest

	testutil.MustParseJSONLines(t, dstenv.RunAndExpectSuccess(t, "snapshot", "list", "-a", sd, "--json"), &manifests)

	if got, want := len(manifests), 1; got != want {
		t.Fatalf("unexpected number of snapshots %v want %v", got, want)
	}

	lines := dstenv.RunAndExpectSuccess(t, "ls", manifests[0].RootObjectID().String())

	// make sure file2.txt was not migrated.
	require.Contains(t, lines, "file1.txt")
	require.NotContains(t, lines, "file2.txt")
}

func writeCompressibleFile(fname string) error {
	f, err := os.Create(fname)
	if err != nil {
		return err
	}

	defer f.Close()

	// 1000 x 64000
	for range 1000 {
		val := uuid.NewString()

		for range 100 {
			if _, err := f.WriteString(val); err != nil {
				return err
			}
		}
	}

	return nil
}
