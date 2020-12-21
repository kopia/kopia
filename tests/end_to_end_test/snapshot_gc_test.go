package endtoend_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotGC(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	expectedContentCount := len(e.RunAndExpectSuccess(t, "content", "list"))

	dataDir := t.TempDir()
	testenv.AssertNoError(t, os.MkdirAll(dataDir, 0o777))
	testenv.AssertNoError(t, ioutil.WriteFile(filepath.Join(dataDir, "some-file1"), []byte(`
hello world
how are you
`), 0o600))

	// take a snapshot of a directory with 1 file
	e.RunAndExpectSuccess(t, "snap", "create", dataDir)

	// data block + directory block + manifest block
	expectedContentCount += 3
	e.RunAndVerifyOutputLineCount(t, expectedContentCount, "content", "list")

	// now delete all manifests, making the content unreachable
	for _, line := range e.RunAndExpectSuccess(t, "snap", "list", "-m") {
		p := strings.Index(line, "manifest:")
		if p >= 0 {
			manifestID := strings.TrimPrefix(strings.Split(line[p:], " ")[0], "manifest:")
			t.Logf("manifestID: %v", manifestID)
			e.RunAndExpectSuccess(t, "manifest", "rm", manifestID)
		}
	}

	// deletion of manifests creates a new manifest
	expectedContentCount++

	// run verification
	e.RunAndExpectSuccess(t, "snapshot", "verify")

	// garbage-collect in dry run mode
	e.RunAndExpectSuccess(t, "snapshot", "gc")

	// data block + directory block + manifest block + manifest block from manifest deletion
	e.RunAndVerifyOutputLineCount(t, expectedContentCount, "content", "list")

	// garbage-collect for real, but contents are too recent so won't be deleted
	e.RunAndExpectSuccess(t, "snapshot", "gc", "--delete")

	// data block + directory block + manifest block + manifest block from manifest deletion
	e.RunAndVerifyOutputLineCount(t, expectedContentCount, "content", "list")

	// make sure we are not too quick
	time.Sleep(2 * time.Second)

	// garbage-collect for real, this time without age limit
	e.RunAndExpectSuccess(t, "snapshot", "gc", "--delete", "--min-age", "0s")

	// two contents are deleted
	expectedContentCount -= 2
	e.RunAndVerifyOutputLineCount(t, expectedContentCount, "content", "list")
}
