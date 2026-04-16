package endtoend_test

import (
	"testing"
	"time"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/tests/testenv"
)

func (s *formatSpecificTestSuite) TestFullMaintenance(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, s.formatFlags, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--disable-repository-log")
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	var (
		snap snapshot.Manifest
		mi   cli.MaintenanceInfo
	)

	e.RunAndExpectSuccess(t, "maintenance", "info")
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &mi)

	// after creation we'll have 1 pack blob
	if got, want := e.RunAndExpectSuccess(t, "blob", "list", "--data-only"), 1; len(got) != want {
		t.Fatalf("unexpected number of initial blobs: %v, want %v", got, want)
	}

	beforeSnapshotBlobs := e.RunAndExpectSuccess(t, "blob", "list", "--data-only")
	_ = beforeSnapshotBlobs

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1, "--json", "--disable-repository-log"), &snap)

	// avoid create and delete in the same second.
	time.Sleep(2 * time.Second)
	e.RunAndExpectSuccess(t, "snapshot", "delete", string(snap.ID), "--delete", "--disable-repository-log")

	e.RunAndVerifyOutputLineCount(t, 0, "snapshot", "list")

	originalBlobs := e.RunAndExpectSuccess(t, "blob", "list", "--data-only")

	e.RunAndExpectSuccess(t, "maintenance", "info")
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &mi)
	e.RunAndVerifyOutputLineCount(t, 0, "maintenance", "run", "--full", "--disable-repository-log")
	e.RunAndExpectSuccess(t, "maintenance", "info")
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &mi)

	if got := e.RunAndExpectSuccess(t, "blob", "list", "--data-only"); len(got) != len(originalBlobs) {
		t.Fatalf("full maintenance is not expected to change any blobs due to safety margins (got %v, was %v)", got, originalBlobs)
	}

	// now rerun with --safety=none
	e.RunAndExpectSuccess(t, "maintenance", "run", "--full", "--safety=none", "--disable-repository-log")
	e.RunAndExpectSuccess(t, "maintenance", "info")
	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "maintenance", "info", "--json"), &mi)

	if got := e.RunAndExpectSuccess(t, "blob", "list", "--data-only"); len(got) >= len(originalBlobs) {
		t.Fatalf("maintenance did not remove blobs: %v, had %v", got, originalBlobs)
	}

	e.RunAndExpectSuccess(t, "content", "list", "-l")

	// we're expecting to have 1 or 2 q blob

	const blobCountAfterFullWipeout = 2

	if got, want := e.RunAndExpectSuccess(t, "blob", "list", "--data-only"), blobCountAfterFullWipeout; len(got) > want {
		t.Fatalf("maintenance left unwanted blobs: %v, want %v", got, want)
	}
}
