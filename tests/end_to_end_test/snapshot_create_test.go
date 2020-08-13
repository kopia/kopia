package endtoend_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotCreate(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	// create some snapshots using different hostname/username
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir, "--override-hostname=foo", "--override-username=foo")
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir3)
	e.RunAndExpectSuccess(t, "snapshot", "list", sharedTestDataDir3)
	e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "connect", "filesystem", "--path", e.RepoDir)

	e.RunAndExpectSuccess(t, "snapshot", "create", ".")
	e.RunAndExpectSuccess(t, "snapshot", "list", ".")

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)

	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	sources := e.ListSnapshotsAndExpectSuccess(t)
	// will only list snapshots we created, not foo@foo
	if got, want := len(sources), 3; got != want {
		t.Errorf("unexpected number of sources: %v, want %v in %#v", got, want, sources)
	}
}

func TestStartTimeOverride(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1, "--start-time", "2000-01-01 01:01:00 UTC")
	sources := e.ListSnapshotsAndExpectSuccess(t)

	gotTime := sources[0].Snapshots[0].Time
	wantTime, _ := time.Parse("2006-01-02 15:04:05 MST", "2000-01-01 01:01:00 UTC")

	if !gotTime.Equal(wantTime) {
		t.Errorf("unexpected start time returned: %v, want %v in %#v", gotTime, wantTime, sources)
	}

	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1, "--start-time", "2000-01-01")
}

func TestEndTimeOverride(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1, "--end-time", "2000-01-01 01:01:00 UTC")
	sources := e.ListSnapshotsAndExpectSuccess(t)

	gotTime := sources[0].Snapshots[0].Time
	wantTime, _ := time.Parse("2006-01-02 15:04:05 MST", "2000-01-01 01:01:00 UTC")

	// If we set the end time then start time should be calculated to be <total snapshot time> seconds before it
	if !gotTime.Before(wantTime) {
		t.Errorf("end time unexpectedly before start time: %v, wanted before %v in %#v", gotTime, wantTime, sources)
	}

	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1, "--end-time", "2000-01-01")
}

func TestInvalidTimeOverride(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1, "--start-time", "2000-01-01 01:01:00 UTC", "--end-time", "1999-01-01 01:01:00 UTC")
}

func TestSnapshottingCacheDirectory(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	lines := e.RunAndExpectSuccess(t, "cache", "info")
	cachePath := filepath.Dir(strings.Split(lines[0], ": ")[0])

	// verify cache marker exists
	if _, err := os.Stat(filepath.Join(cachePath, repo.CacheDirMarkerFile)); err != nil {
		t.Fatal(err)
	}

	e.RunAndExpectSuccess(t, "snapshot", "create", cachePath)
	snapshots := e.ListSnapshotsAndExpectSuccess(t, cachePath)

	rootID := snapshots[0].Snapshots[0].ObjectID
	if got, want := len(e.RunAndExpectSuccess(t, "ls", rootID)), 0; got != want {
		t.Errorf("invalid number of files in snapshot of cache dir %v, want %v", got, want)
	}
}
