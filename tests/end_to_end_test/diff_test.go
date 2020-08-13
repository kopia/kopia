package endtoend_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestDiff(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	dataDir := makeScratchDir(t)

	// initial snapshot
	testenv.AssertNoError(t, os.MkdirAll(dataDir, 0o777))
	e.RunAndExpectSuccess(t, "snapshot", "create", dataDir)

	// create some directories and files
	testenv.AssertNoError(t, os.MkdirAll(filepath.Join(dataDir, "foo"), 0o700))
	testenv.AssertNoError(t, ioutil.WriteFile(filepath.Join(dataDir, "some-file1"), []byte(`
hello world
how are you
`), 0o600))
	testenv.AssertNoError(t, ioutil.WriteFile(filepath.Join(dataDir, "some-file2"), []byte(`
quick brown
fox jumps
over the lazy
dog
`), 0o600))
	e.RunAndExpectSuccess(t, "snapshot", "create", dataDir)

	// change some files
	testenv.AssertNoError(t, ioutil.WriteFile(filepath.Join(dataDir, "some-file2"), []byte(`
quick brown
fox jumps
over the lazy
canary
`), 0o600))

	testenv.AssertNoError(t, os.MkdirAll(filepath.Join(dataDir, "bar"), 0o700))
	e.RunAndExpectSuccess(t, "snapshot", "create", dataDir)

	// change some files
	os.Remove(filepath.Join(dataDir, "some-file1"))

	testenv.AssertNoError(t, os.MkdirAll(filepath.Join(dataDir, "bar"), 0o700))
	e.RunAndExpectSuccess(t, "snapshot", "create", dataDir)

	si := e.ListSnapshotsAndExpectSuccess(t, dataDir)
	if got, want := len(si), 1; got != want {
		t.Fatalf("got %v sources, wanted %v", got, want)
	}

	// make sure we can generate between all versions of the directory
	snapshots := si[0].Snapshots
	for _, s1 := range snapshots {
		for _, s2 := range snapshots {
			e.RunAndExpectSuccess(t, "diff", "-f", s1.ObjectID, s2.ObjectID)
		}
	}
}
