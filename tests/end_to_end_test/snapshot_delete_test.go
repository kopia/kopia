package endtoend_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testdirtree"
	"github.com/kopia/kopia/tests/testenv"
)

type deleteArgMaker func(manifestID, objectID string, source clitestutil.SourceInfo) []string

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
			func(manifestID, objectID string, source clitestutil.SourceInfo) []string {
				return []string{"manifest", "rm", manifestID}
			},
			expectSuccess,
		},
		{
			"Dry run - by manifest ID",
			func(manifestID, objectID string, source clitestutil.SourceInfo) []string {
				return []string{"snapshot", "delete", manifestID}
			},
			expectSuccess,
		},
		{
			"Delete - by manifest ID",
			func(manifestID, objectID string, source clitestutil.SourceInfo) []string {
				return []string{"snapshot", "delete", manifestID, "--delete"}
			},
			expectSuccess,
		},
		{
			"Delete - by manifest ID - legacy flag",
			func(manifestID, objectID string, source clitestutil.SourceInfo) []string {
				return []string{"snapshot", "delete", manifestID, "--unsafe-ignore-source"}
			},
			expectSuccess,
		},
		{
			"Dry run - by objectID ID",
			func(manifestID, objectID string, source clitestutil.SourceInfo) []string {
				return []string{"snapshot", "delete", objectID}
			},
			expectSuccess,
		},
		{
			"Delete - by object ID",
			func(manifestID, objectID string, source clitestutil.SourceInfo) []string {
				return []string{"snapshot", "delete", objectID, "--delete"}
			},
			expectSuccess,
		},
		{
			"Dry run - invalid object ID",
			func(manifestID, objectID string, source clitestutil.SourceInfo) []string {
				return []string{"snapshot", "delete", "no-such-manifest"}
			},
			expectFail,
		},
		{
			"Delete - invalid manifest ID",
			func(manifestID, objectID string, source clitestutil.SourceInfo) []string {
				return []string{"snapshot", "delete", "no-such-manifest", "--delete"}
			},
			expectFail,
		},
		{
			"Dry run - invalid object ID",
			func(manifestID, objectID string, source clitestutil.SourceInfo) []string {
				return []string{"snapshot", "delete", "k001122"}
			},
			expectFail,
		},
		{
			"Delete - invalid object ID",
			func(manifestID, objectID string, source clitestutil.SourceInfo) []string {
				return []string{"snapshot", "delete", "k001122", "--delete"}
			},
			expectFail,
		},
	} {
		t.Log(tc.desc)
		testSnapshotDelete(t, tc.mf, tc.expectSuccess)
	}
}

func testSnapshotDelete(t *testing.T, argMaker deleteArgMaker, expectDeleteSucceeds bool) {
	t.Helper()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	dataDir := testutil.TempDirectory(t)
	require.NoError(t, os.MkdirAll(dataDir, 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "some-file1"), []byte(`
hello world
how are you
`), 0o600))

	// take a snapshot of a directory with 1 file
	e.RunAndExpectSuccess(t, "snap", "create", dataDir)

	// now delete all manifests, making the content unreachable
	si := clitestutil.ListSnapshotsAndExpectSuccess(t, e, dataDir)
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

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

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

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := filepath.Join(testutil.TempDirectory(t), "source")
	testdirtree.MustCreateDirectoryTree(t, source, testdirtree.DirectoryTreeOptions{
		Depth:                  1,
		MaxSubdirsPerDirectory: 10,
		MaxFilesPerDirectory:   10,
	})

	// Create snapshot
	e.RunAndExpectSuccess(t, "snapshot", "create", source)

	// obtain snapshot root id and use it for restore
	si := clitestutil.ListSnapshotsAndExpectSuccess(t, e, source)
	if got, want := len(si), 1; got != want {
		t.Fatalf("got %v sources, wanted %v", got, want)
	}

	if got, want := len(si[0].Snapshots), 1; got != want {
		t.Fatalf("got %v snapshots, wanted %v", got, want)
	}

	snapID := si[0].Snapshots[0].SnapshotID
	rootID := si[0].Snapshots[0].ObjectID

	restoreDir := testutil.TempDirectory(t)
	e.RunAndExpectSuccess(t, "restore", rootID, restoreDir)

	// Note: restore does not reset the permissions for the top directory due to
	// the way the top FS entry is created in snapshotfs. Force the permissions
	// of the top directory to match those of the source so the recursive
	// directory comparison has a chance of succeeding.
	require.NoError(t, os.Chmod(restoreDir, 0o700))
	compareDirs(t, source, restoreDir)

	// snapshot delete should succeed
	e.RunAndExpectSuccess(t, "snapshot", "delete", snapID, "--delete")

	notRestoreDir := testutil.TempDirectory(t)

	// Make sure the restore did not happen from the deleted snapshot
	e.RunAndExpectFailure(t, "snapshot", "restore", snapID, notRestoreDir)
	assertEmptyDir(t, notRestoreDir)

	// Subsequent snapshot delete to the same ID should fail
	e.RunAndExpectFailure(t, "snapshot", "delete", snapID, "--delete")

	// garbage-collect to clean up the root object.
	e.RunAndExpectSuccess(t, "maintenance", "run", "--full")

	// Run a restore on the deleted snapshot's root ID. The root should be
	// marked as deleted but still recoverable
	restoreDir2 := testutil.TempDirectory(t)

	e.RunAndExpectSuccess(t, "restore", rootID, restoreDir2)
	require.NoError(t, os.Chmod(restoreDir2, 0o700))
	compareDirs(t, source, restoreDir2)
}

func assertEmptyDir(t *testing.T, dir string) {
	t.Helper()

	// Make sure the restore did not happen from the deleted snapshot
	dirEntries, err := os.ReadDir(dir)
	require.NoError(t, err)

	if len(dirEntries) != 0 {
		t.Fatalf("expected nothing to be restored")
	}
}

func TestDeleteAllSnapshotsForSource(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir2)

	// first source is header + 3 lines, 2nd source is header + 2 lines + separator between them.
	e.RunAndVerifyOutputLineCount(t, 8, "snapshot", "list", "-l")

	// failure cases
	e.RunAndExpectFailure(t, "snapshot", "delete", "--all-snapshots-for-source", "no-such-user@no-such-host:/tmp")
	e.RunAndExpectFailure(t, "snapshot", "delete", "--all-snapshots-for-source", testutil.TempDirectory(t))

	// dry run has no effect
	e.RunAndExpectSuccess(t, "snapshot", "delete", "--all-snapshots-for-source", sharedTestDataDir2)
	e.RunAndVerifyOutputLineCount(t, 8, "snapshot", "list", "-l")

	// passing --delete actually removes snapshots
	e.RunAndExpectSuccess(t, "snapshot", "delete", "--all-snapshots-for-source", sharedTestDataDir2, "--delete")
	e.RunAndVerifyOutputLineCount(t, 4, "snapshot", "list", "-l")
	e.RunAndExpectSuccess(t, "snapshot", "delete", "--all-snapshots-for-source", sharedTestDataDir1, "--delete")
	e.RunAndVerifyOutputLineCount(t, 0, "snapshot", "list", "-l")
}
