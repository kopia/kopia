package endtoend_test

import (
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestIndexOptimize(t *testing.T) {
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

	e.RunAndVerifyOutputLineCount(t, 6, "index", "ls")
	e.RunAndExpectSuccess(t, "index", "optimize")
	e.RunAndVerifyOutputLineCount(t, 1, "index", "ls")

	e.RunAndExpectSuccess(t, "snapshot", "create", ".", sharedTestDataDir1, sharedTestDataDir2)

	// we flush individually after each snapshot source, so this adds 3 indexes
	e.RunAndVerifyOutputLineCount(t, 4, "index", "ls")
}
