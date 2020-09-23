package endtoend_test

import (
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestRepositoryRepair(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	e.RunAndExpectSuccess(t, "snapshot", "create", ".")
	e.RunAndExpectSuccess(t, "snapshot", "list", ".")

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	// remove kopia.repository
	e.RunAndExpectSuccess(t, "blob", "rm", "kopia.repository")
	e.RunAndExpectSuccess(t, "repo", "disconnect")

	// this will fail because the format blob in the repository is not found
	e.RunAndExpectFailure(t, "repo", "connect", "filesystem", "--path", e.RepoDir)

	// now run repair, which will recover the format blob from one of the pack blobs.
	e.RunAndExpectSuccess(t, "repo", "repair", "--log-level=debug", "--trace-storage", "filesystem", "--path", e.RepoDir)

	// now connect can succeed
	e.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.RepoDir)
}
