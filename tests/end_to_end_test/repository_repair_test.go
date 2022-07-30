package endtoend_test

import (
	"testing"

	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/tests/testenv"
)

// when password change is enabled, replicas of kopia.repository are not embedded in blobs
// so `kopia repository repair` will not work.
func (s *formatSpecificTestSuite) TestRepositoryRepair(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, s.formatFlags, runner)

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

	if s.formatVersion == format.FormatVersion1 {
		// now run repair, which will recover the format blob from one of the pack blobs.
		e.RunAndExpectSuccess(t, "repo", "repair", "filesystem", "--path", e.RepoDir)

		// now connect can succeed
		e.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.RepoDir)
	}
}
