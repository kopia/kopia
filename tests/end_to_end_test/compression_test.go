package endtoend_test

import (
	"io/ioutil"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestCompression(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	// set global policy
	e.RunAndExpectSuccess(t, "policy", "set", "--global", "--compression", "pgzip")

	dataDir := makeScratchDir(t)

	dataLines := []string{
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
		"hello world",
		"how are you",
	}
	// add a file that compresses well
	testenv.AssertNoError(t, ioutil.WriteFile(filepath.Join(dataDir, "some-file1"), []byte(strings.Join(dataLines, "\n")), 0o600))

	e.RunAndExpectSuccess(t, "snapshot", "create", dataDir)
	sources := e.ListSnapshotsAndExpectSuccess(t)
	oid := sources[0].Snapshots[0].ObjectID
	entries := e.ListDirectory(t, oid)

	if !strings.HasPrefix(entries[0].ObjectID, "Z") {
		t.Errorf("expected compressed object, got %v", entries[0].ObjectID)
	}

	if lines := e.RunAndExpectSuccess(t, "show", entries[0].ObjectID); !reflect.DeepEqual(dataLines, lines) {
		t.Errorf("invalid object contents")
	}
}
