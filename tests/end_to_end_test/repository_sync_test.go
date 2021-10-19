package endtoend_test

import (
	"testing"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testenv"
)

func TestRepositorySync(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	sources := clitestutil.ListSnapshotsAndExpectSuccess(t, e)

	// synchronize repository blobs to another directory
	dir2 := testutil.TempDirectory(t)
	e.RunAndExpectSuccess(t, "repo", "sync-to", "filesystem", "--path", dir2, "--times")

	// change some parameter in the repository format and make sure we can still synchronize.
	// this is equivalent to an upgrade.
	e.RunAndExpectSuccess(t, "repo", "set-parameters", "--max-pack-size-mb", "21")
	e.RunAndExpectSuccess(t, "repo", "sync-to", "filesystem", "--path", dir2, "--times")

	// synchronizing to empty directory fails with --must-exist
	dir3 := testutil.TempDirectory(t)
	e.RunAndExpectFailure(t, "repo", "sync-to", "filesystem", "--path", dir3, "--must-exist")

	// now connect to the new repository in new location
	e.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", dir2)

	// snapshot list should be the same
	sources2 := clitestutil.ListSnapshotsAndExpectSuccess(t, e)
	if got, want := len(sources2), len(sources); got != want {
		t.Errorf("unexpected number of sources: %v, want %v in %#v", got, want, sources2)
	}

	// now create a whole new repository
	e2 := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e2.RunAndExpectSuccess(t, "repo", "disconnect")

	e2.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e2.RepoDir)

	// syncing to the directory should fail because it contains incompatible format blob.
	e2.RunAndExpectFailure(t, "repo", "sync-to", "filesystem", "--path", dir2)
}
