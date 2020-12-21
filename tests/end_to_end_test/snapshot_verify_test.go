package endtoend_test

import (
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotVerifyTest(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	e.RunAndExpectSuccess(t, "snap", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snap", "verify")

	// list blobs and remove the first 'p', don't remove 'q' or anything else since
	// we may delete the record of snapshot itself.
	for _, line := range e.RunAndExpectSuccess(t, "blob", "ls") {
		blobID := strings.Fields(line)[0]
		if strings.HasPrefix(blobID, "p") {
			e.RunAndExpectSuccess(t, "blob", "rm", blobID)
			break
		}
	}

	e.RunAndExpectFailure(t, "snap", "verify")
}
