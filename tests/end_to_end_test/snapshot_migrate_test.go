package endtoend_test

import (
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotMigrate(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "snapshot", "create", ".")

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir3)

	dstenv := testenv.NewCLITest(t)
	defer dstenv.Cleanup(t)

	dstenv.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", dstenv.RepoDir)
	dstenv.RunAndExpectSuccess(t, "snapshot", "migrate", "--source-config", filepath.Join(e.ConfigDir, ".kopia.config"), "--all")
	// migrate again, which should be a no-op.
	dstenv.RunAndExpectSuccess(t, "snapshot", "migrate", "--source-config", filepath.Join(e.ConfigDir, ".kopia.config"), "--all")

	sourceSnapshotCount := len(e.RunAndExpectSuccess(t, "snapshot", "list", ".", "-a"))
	dstenv.RunAndVerifyOutputLineCount(t, sourceSnapshotCount, "snapshot", "list", ".", "-a")
}
