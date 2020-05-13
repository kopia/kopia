package endtoend_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

type deleteArgMaker func(manifestID, objectID string, source testenv.SourceInfo) []string

//nolint:funlen
func TestSnapshotDelete(t *testing.T) {
	t.Parallel()

	const (
		expectFail    = false
		expectSuccess = true
	)

	for _, tc := range []struct {
		desc          string
		mf            deleteArgMaker
		expectSuccess bool
	}{
		{
			"Test manifest rm function",
			func(manifestID, objectID string, source testenv.SourceInfo) []string {
				return []string{"manifest", "rm", manifestID}
			},
			expectSuccess,
		},
		{
			"Dry run - by manifest ID",
			func(manifestID, objectID string, source testenv.SourceInfo) []string {
				return []string{"snapshot", "delete", manifestID}
			},
			expectSuccess,
		},
		{
			"Delete - by manifest ID",
			func(manifestID, objectID string, source testenv.SourceInfo) []string {
				return []string{"snapshot", "delete", manifestID, "--delete"}
			},
			expectSuccess,
		},
		{
			"Delete - by manifest ID - legacy flag",
			func(manifestID, objectID string, source testenv.SourceInfo) []string {
				return []string{"snapshot", "delete", manifestID, "--unsafe-ignore-source"}
			},
			expectSuccess,
		},
		{
			"Dry run - by objectID ID",
			func(manifestID, objectID string, source testenv.SourceInfo) []string {
				return []string{"snapshot", "delete", objectID}
			},
			expectSuccess,
		},
		{
			"Delete - by object ID",
			func(manifestID, objectID string, source testenv.SourceInfo) []string {
				return []string{"snapshot", "delete", objectID, "--delete"}
			},
			expectSuccess,
		}, {
			"Dry run - invalid object ID",
			func(manifestID, objectID string, source testenv.SourceInfo) []string {
				return []string{"snapshot", "delete", "no-such-manifest"}
			},
			expectFail,
		},
		{
			"Delete - invalid manifest ID",
			func(manifestID, objectID string, source testenv.SourceInfo) []string {
				return []string{"snapshot", "delete", "no-such-manifest", "--delete"}
			},
			expectFail,
		},
		{
			"Dry run - invalid object ID",
			func(manifestID, objectID string, source testenv.SourceInfo) []string {
				return []string{"snapshot", "delete", "k001122"}
			},
			expectFail,
		},
		{
			"Delete - invalid object ID",
			func(manifestID, objectID string, source testenv.SourceInfo) []string {
				return []string{"snapshot", "delete", "k001122", "--delete"}
			},
			expectFail,
		}} {
		t.Log(tc.desc)
		testSnapshotDelete(t, tc.mf, tc.expectSuccess)
	}
}

func testSnapshotDelete(t *testing.T, argMaker deleteArgMaker, expectDeleteSucceeds bool) {
	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	dataDir := makeScratchDir(t)
	testenv.AssertNoError(t, os.MkdirAll(dataDir, 0777))
	testenv.AssertNoError(t, ioutil.WriteFile(filepath.Join(dataDir, "some-file1"), []byte(`
hello world
how are you
`), 0600))

	// take a snapshot of a directory with 1 file
	e.RunAndExpectSuccess(t, "snap", "create", dataDir)

	// now delete all manifests, making the content unreachable
	si := e.ListSnapshotsAndExpectSuccess(t, dataDir)
	for _, source := range si {
		for _, ss := range source.Snapshots {
			manifestID := ss.SnapshotID
			args := argMaker(manifestID, ss.ObjectID, source)

			if expectDeleteSucceeds {
				e.RunAndExpectSuccess(t, args...)
			} else {
				e.RunAndExpectFailure(t, args...)
			}
		}
	}
}

func TestSnapshotDeleteTypeCheck(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	lines := e.RunAndExpectSuccess(t, "manifest", "ls")
	if len(lines) != 2 {
		t.Fatalf("Expected 2 line global policy + maintenance config output for manifest ls")
	}

	for _, line := range lines {
		fields := strings.Fields(line)
		manifestID := fields[0]
		typeField := fields[5]

		typeVal := strings.TrimPrefix(typeField, "type:")
		if typeVal == "maintenance" {
			continue
		}

		if typeVal != "policy" {
			t.Fatalf("Expected global policy manifest on a fresh repo")
		}

		e.RunAndExpectFailure(t, "snapshot", "delete", manifestID)
	}
}

func TestSnapshotDeleteRestore(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := makeScratchDir(t)
	testenv.MustCreateDirectoryTree(t, source, testenv.DirectoryTreeOptions{
		Depth:                  1,
		MaxSubdirsPerDirectory: 10,
		MaxFilesPerDirectory:   10,
	})

	// Create snapshot
	e.RunAndExpectSuccess(t, "snapshot", "create", source)

	// obtain snapshot root id and use it for restore
	si := e.ListSnapshotsAndExpectSuccess(t, source)
	if got, want := len(si), 1; got != want {
		t.Fatalf("got %v sources, wanted %v", got, want)
	}

	if got, want := len(si[0].Snapshots), 1; got != want {
		t.Fatalf("got %v snapshots, wanted %v", got, want)
	}

	snapID := si[0].Snapshots[0].SnapshotID
	rootID := si[0].Snapshots[0].ObjectID

	restoreDir := makeScratchDir(t)
	e.RunAndExpectSuccess(t, "restore", rootID, restoreDir)

	// Note: restore does not reset the permissions for the top directory due to
	// the way the top FS entry is created in snapshotfs. Force the permissions
	// of the top directory to match those of the source so the recursive
	// directory comparison has a chance of succeeding.
	testenv.AssertNoError(t, os.Chmod(restoreDir, 0700))
	compareDirs(t, source, restoreDir)

	// snapshot delete should succeed
	e.RunAndExpectSuccess(t, "snapshot", "delete", snapID, "--delete")

	// Subsequent snapshot delete to the same ID should fail
	e.RunAndExpectFailure(t, "snapshot", "delete", snapID, "--delete")

	// garbage-collect to clean up the root object. Otherwise
	// a restore will succeed
	e.RunAndExpectSuccess(t, "snapshot", "gc", "--delete", "--min-age", "0s")

	// Run a restore on the deleted snapshot's root ID
	notRestoreDir := makeScratchDir(t)
	e.RunAndExpectFailure(t, "restore", rootID, notRestoreDir)

	// Make sure the restore did not happen from the deleted snapshot
	fileInfo, err := ioutil.ReadDir(notRestoreDir)
	testenv.AssertNoError(t, err)

	if len(fileInfo) != 0 {
		t.Fatalf("expected nothing to be restored")
	}
}
