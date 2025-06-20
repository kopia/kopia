package endtoend_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotCreateCombine(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	// Test that --combine requires multiple sources
	e.RunAndExpectFailure(t, "snapshot", "create", sharedTestDataDir1, "--combine")

	// Test combining multiple existing test data directories
	var manifest snapshot.Manifest

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1, sharedTestDataDir2, "--combine", "--json"), &manifest)

	require.NotEmpty(t, manifest.ID)
	require.NotEmpty(t, manifest.RootEntry.ObjectID)

	// Verify it created only one snapshot (combined)
	var snapshots []cli.SnapshotManifest

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "list", "--json"), &snapshots)
	require.Len(t, snapshots, 1)

	// Test that without --combine, multiple sources create multiple snapshots
	e.RunAndExpectSuccess(t, "snapshot", "create", sharedTestDataDir1, sharedTestDataDir2)

	// Now we should have 3 total snapshots (1 combined + 2 individual)
	var snapshots2 []cli.SnapshotManifest

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "list", "--json"), &snapshots2)
	require.Len(t, snapshots2, 3)
}

func TestSnapshotCreateCombineFiles(t *testing.T) {
	// Cannot run in parallel due to os.Chdir
	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	// Create test files in the current directory
	testDir := testutil.TempDirectory(t)
	file1 := filepath.Join(testDir, "file1.txt")
	file2 := filepath.Join(testDir, "file2.txt")

	require.NoError(t, os.WriteFile(file1, []byte("content of file 1"), 0o644))
	require.NoError(t, os.WriteFile(file2, []byte("content of file 2"), 0o644))

	// Test combining files
	var manifest snapshot.Manifest

	// Change to the test directory so files are at root level
	oldCwd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(testDir))

	defer os.Chdir(oldCwd)

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create",
		"file1.txt", "file2.txt", "--combine", "--json"), &manifest)

	require.NotEmpty(t, manifest.ID)
	require.NotEmpty(t, manifest.RootEntry.ObjectID)

	// Verify the snapshot contents
	output := e.RunAndExpectSuccess(t, "ls", manifest.RootEntry.ObjectID.String())
	outputStr := strings.Join(output, "\n")
	require.Contains(t, outputStr, "file1.txt")
	require.Contains(t, outputStr, "file2.txt")

	// Verify file contents are preserved
	file1Output := e.RunAndExpectSuccess(t, "show", manifest.RootEntry.ObjectID.String()+"/file1.txt")
	require.Contains(t, strings.Join(file1Output, "\n"), "content of file 1")
}

func TestSnapshotCreateCombineMixedFilesAndDirs(t *testing.T) {
	// Cannot run in parallel due to os.Chdir
	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	// Create test structure with files and directories
	testDir := testutil.TempDirectory(t)

	// Create a file at root
	file1 := filepath.Join(testDir, "root-file.txt")
	require.NoError(t, os.WriteFile(file1, []byte("root file content"), 0o644))

	// Create a directory with files
	subDir := filepath.Join(testDir, "subdir")
	require.NoError(t, os.MkdirAll(subDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(subDir, "sub-file.txt"), []byte("sub file content"), 0o644))

	// Test combining file and directory
	var manifest snapshot.Manifest

	// Change to test directory
	oldCwd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(testDir))

	defer os.Chdir(oldCwd)

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create",
		"root-file.txt", "subdir", "--combine", "--json"), &manifest)

	require.NotEmpty(t, manifest.ID)
	require.NotEmpty(t, manifest.RootEntry.ObjectID)

	// Verify the snapshot structure
	output := e.RunAndExpectSuccess(t, "ls", manifest.RootEntry.ObjectID.String())
	outputStr := strings.Join(output, "\n")
	require.Contains(t, outputStr, "root-file.txt")
	require.Contains(t, outputStr, "subdir")

	// Verify subdirectory contents
	subdirOutput := e.RunAndExpectSuccess(t, "ls", manifest.RootEntry.ObjectID.String()+"/subdir")
	subdirOutputStr := strings.Join(subdirOutput, "\n")
	require.Contains(t, subdirOutputStr, "sub-file.txt")
}

func TestSnapshotCreateCombinePathConflict(t *testing.T) {
	// Cannot run in parallel due to os.Chdir
	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	// Create test files with the same name
	testDir := testutil.TempDirectory(t)

	// Create two files with the same name in different directories
	dir1 := filepath.Join(testDir, "dir1")
	dir2 := filepath.Join(testDir, "dir2")

	require.NoError(t, os.MkdirAll(dir1, 0o755))
	require.NoError(t, os.MkdirAll(dir2, 0o755))

	file1 := filepath.Join(dir1, "same.txt")
	file2 := filepath.Join(dir2, "same.txt")

	require.NoError(t, os.WriteFile(file1, []byte("content 1"), 0o644))
	require.NoError(t, os.WriteFile(file2, []byte("content 2"), 0o644))

	// Try to combine files with the same basename but different paths - should succeed
	// because they're in different directories
	oldCwd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(testDir))

	defer os.Chdir(oldCwd)

	// This should succeed because the files are in different directories
	var manifest snapshot.Manifest

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create",
		filepath.Join("dir1", "same.txt"),
		filepath.Join("dir2", "same.txt"),
		"--combine", "--json"), &manifest)

	// Verify both files exist in their respective directories
	// List dir1 contents
	dir1Output := e.RunAndExpectSuccess(t, "ls", manifest.RootEntry.ObjectID.String()+"/dir1")
	require.Contains(t, strings.Join(dir1Output, "\n"), "same.txt")

	// List dir2 contents
	dir2Output := e.RunAndExpectSuccess(t, "ls", manifest.RootEntry.ObjectID.String()+"/dir2")
	require.Contains(t, strings.Join(dir2Output, "\n"), "same.txt")

	// Now test actual path conflict - try to combine the same file twice
	e.RunAndExpectFailure(t, "snapshot", "create",
		"dir1/same.txt",
		"dir1/same.txt",
		"--combine")
}

func TestSnapshotCreateCombineComplexHierarchy(t *testing.T) {
	// Cannot run in parallel due to os.Chdir
	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	// Create a complex directory structure
	testDir := testutil.TempDirectory(t)

	// Create structure:
	// testDir/
	//   file1.txt
	//   dir1/
	//     file2.txt
	//     subdir1/
	//       file3.txt
	//   dir2/
	//     file4.txt

	require.NoError(t, os.WriteFile(filepath.Join(testDir, "file1.txt"), []byte("file1"), 0o644))

	dir1 := filepath.Join(testDir, "dir1")
	require.NoError(t, os.MkdirAll(dir1, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir1, "file2.txt"), []byte("file2"), 0o644))

	subdir1 := filepath.Join(dir1, "subdir1")
	require.NoError(t, os.MkdirAll(subdir1, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(subdir1, "file3.txt"), []byte("file3"), 0o644))

	dir2 := filepath.Join(testDir, "dir2")
	require.NoError(t, os.MkdirAll(dir2, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir2, "file4.txt"), []byte("file4"), 0o644))

	// Combine different parts of the hierarchy
	var manifest snapshot.Manifest

	oldCwd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(testDir))

	defer os.Chdir(oldCwd)

	testutil.MustParseJSONLines(t, e.RunAndExpectSuccess(t, "snapshot", "create",
		"file1.txt",
		"dir1/subdir1",
		"dir2",
		"--combine", "--json"), &manifest)

	require.NotEmpty(t, manifest.ID)
	require.NotEmpty(t, manifest.RootEntry.ObjectID)

	// Verify the structure
	output := e.RunAndExpectSuccess(t, "ls", manifest.RootEntry.ObjectID.String())
	outputStr := strings.Join(output, "\n")
	require.Contains(t, outputStr, "file1.txt")
	require.Contains(t, outputStr, "dir1")
	require.Contains(t, outputStr, "dir2")

	// Verify dir1 has subdir1
	dir1Output := e.RunAndExpectSuccess(t, "ls", manifest.RootEntry.ObjectID.String()+"/dir1")
	dir1OutputStr := strings.Join(dir1Output, "\n")
	require.Contains(t, dir1OutputStr, "subdir1")

	// Verify subdir1 contents
	subdir1Output := e.RunAndExpectSuccess(t, "ls", manifest.RootEntry.ObjectID.String()+"/dir1/subdir1")
	subdir1OutputStr := strings.Join(subdir1Output, "\n")
	require.Contains(t, subdir1OutputStr, "file3.txt")
}
