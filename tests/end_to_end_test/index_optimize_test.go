package endtoend_test

import (
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func (s *formatSpecificTestSuite) TestIndexOptimize(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, s.formatFlags, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	for _, line := range e.RunAndExpectSuccess(t, "repository", "status") {
		if strings.HasPrefix(line, "Epoch Manager:") && strings.Contains(line, "enabled") {
			t.Skip()
		}
	}

	e.RunAndExpectSuccess(t, "snapshot", "create", ".")

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	e.RunAndVerifyOutputLineCount(t, 6, "index", "ls")
	e.RunAndExpectSuccess(t, "index", "optimize")
	e.RunAndVerifyOutputLineCount(t, 1, "index", "ls")

	e.RunAndExpectSuccess(t, "snapshot", "create", ".", sharedTestDataDir1, sharedTestDataDir2, "--flush-per-source")

	// we flush individually after each snapshot source, so this adds 3 indexes
	e.RunAndVerifyOutputLineCount(t, 4, "index", "ls")
}
