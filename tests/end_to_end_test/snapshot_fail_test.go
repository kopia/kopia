package endtoend_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotNonexistent(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	scratchDir := makeScratchDir(t)

	// Test snapshot of nonexistent directory fails
	e.RunAndExpectFailure(t, "snapshot", "create", filepath.Join(scratchDir, "notExist"))
}

func TestSnapshotFail(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("this test does not work on Windows")
	}

	dir0Path := "dir0"

	for _, ignoreFileErr := range []string{"true", "false"} {
		for _, ignoreDirErr := range []string{"true", "false"} {
			ignoringDirs := ignoreDirErr == "true"
			ignoringFiles := ignoreFileErr == "true"

			// Use "inherit" instead of "false" sometimes. Inherit defaults to false
			if !ignoringFiles && rand.Intn(2) == 0 {
				ignoreFileErr = "inherit"
			}

			if !ignoringDirs && rand.Intn(2) == 0 {
				ignoreDirErr = "inherit"
			}

			// Test the root dir permissions
			for ti, tt := range []struct {
				desc          string
				modifyEntry   string
				snapSource    string
				expectSuccess map[os.FileMode]bool
			}{
				{
					desc:        "Modify permissions of the parent dir of the snapshot source (source is a FILE)",
					modifyEntry: dir0Path,
					snapSource:  filepath.Join(dir0Path, "file1"),
					expectSuccess: map[os.FileMode]bool{
						0000: false, // --- permission: cannot read directory
						0100: true,  // --X permission: can enter directory and take snapshot of the file (with full permissions)
						0400: false, // R-- permission: can read the file name, but will be unable to snapshot it without entering directory
					},
				},
				{
					desc:        "Modify permissions of the parent dir of the snapshot source (source is a DIRECTORY)",
					modifyEntry: dir0Path,
					snapSource:  filepath.Join(dir0Path, "dir1"),
					expectSuccess: map[os.FileMode]bool{
						0000: false,
						0100: true,
						0400: false,
					},
				},
				{
					desc:        "Modify permissions of the parent dir of the snapshot source (source is an EMPTY directory)",
					modifyEntry: dir0Path,
					snapSource:  filepath.Join(dir0Path, "emptyDir1"),
					expectSuccess: map[os.FileMode]bool{
						0000: false,
						0100: true,
						0400: false,
					},
				},
				{
					desc:        "Modify permissions of the snapshot source itself (source is a FILE)",
					modifyEntry: filepath.Join(dir0Path, "file1"),
					snapSource:  filepath.Join(dir0Path, "file1"),
					expectSuccess: map[os.FileMode]bool{
						0000: false,
						0100: false,
						0400: true,
					},
				},
				{
					desc:        "Modify permissions of the snapshot source itself (source is a DIRECTORY)",
					modifyEntry: filepath.Join(dir0Path, "dir1"),
					snapSource:  filepath.Join(dir0Path, "dir1"),
					expectSuccess: map[os.FileMode]bool{
						0000: false,
						0100: false,
						0400: false,
					},
				},
				{
					desc:        "Modify permissions of the snapshot source itself (source is an EMPTY directory)",
					modifyEntry: filepath.Join(dir0Path, "emptyDir1"),
					snapSource:  filepath.Join(dir0Path, "emptyDir1"),
					expectSuccess: map[os.FileMode]bool{
						0000: false,
						0100: false,
						0400: true,
					},
				},
				{
					desc:        "Modify permissions of a FILE in the snapshot directory",
					modifyEntry: filepath.Join(dir0Path, "dir1", "file2"),
					snapSource:  filepath.Join(dir0Path, "dir1"),
					expectSuccess: map[os.FileMode]bool{
						0000: ignoringFiles,
						0100: ignoringFiles,
						0400: true,
					},
				},
				{
					desc:        "Modify permissions of a DIRECTORY in the snapshot directory",
					modifyEntry: filepath.Join(dir0Path, "dir1", "dir2"),
					snapSource:  filepath.Join(dir0Path, "dir1"),
					expectSuccess: map[os.FileMode]bool{
						0000: ignoringDirs,
						0100: ignoringDirs,
						0400: ignoringDirs,
					},
				},
				{
					desc:        "Modify permissions of an EMPTY directory in the snapshot directory",
					modifyEntry: filepath.Join(dir0Path, "dir1", "emptyDir2"),
					snapSource:  filepath.Join(dir0Path, "dir1"),
					expectSuccess: map[os.FileMode]bool{
						0000: ignoringDirs,
						0100: ignoringDirs,
						0400: true,
					},
				},
				{
					desc:        "Modify permissions of a FILE in a subdirectory of the snapshot root directory",
					modifyEntry: filepath.Join(dir0Path, "dir1", "file2"),
					snapSource:  dir0Path,
					expectSuccess: map[os.FileMode]bool{
						0000: ignoringFiles,
						0100: ignoringFiles,
						0400: true,
					},
				},
				{
					desc:        "Modify permissions of a DIRECTORY in a subdirectory of the snapshot root directory",
					modifyEntry: filepath.Join(dir0Path, "dir1", "dir2"),
					snapSource:  dir0Path,
					expectSuccess: map[os.FileMode]bool{
						0000: ignoringDirs,
						0100: ignoringDirs,
						0400: ignoringDirs,
					},
				},
				{
					desc:        "Modify permissions of an EMPTY directory in a subdirectory of the snapshot root directory",
					modifyEntry: filepath.Join(dir0Path, "dir1", "emptyDir2"),
					snapSource:  dir0Path,
					expectSuccess: map[os.FileMode]bool{
						0000: ignoringDirs,
						0100: ignoringDirs,
						0400: true,
					},
				},
			} {
				// Reference test conditions outside of range variables to satisfy linter
				tcIgnoreDirErr := ignoreDirErr
				tcIgnoreFileErr := ignoreFileErr
				tcIdx := ti
				tc := tt
				tname := fmt.Sprintf("%s_ignoreFileErr_%s_ignoreDirErr_%s", tc.desc, ignoreDirErr, ignoreFileErr)

				t.Run(tname, func(t *testing.T) {
					t.Parallel()

					e := testenv.NewCLITest(t)
					defer e.Cleanup(t)
					defer e.RunAndExpectSuccess(t, "repo", "disconnect")

					e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

					scratchDir := makeScratchDir(t)

					snapSource := filepath.Join(scratchDir, tc.snapSource)
					modifyEntry := filepath.Join(scratchDir, tc.modifyEntry)

					// Each directory tier will have a file, an empty directory, and the next tier's directory
					// (unless at max depth). Naming scheme is [file|dir|emptyDir][tier #].
					createSimplestFileTree(t, 3, 0, scratchDir)

					restoreDirPrefix := filepath.Join(scratchDir, "target")

					e.RunAndExpectSuccess(t, "policy", "set", snapSource, "--ignore-dir-errors", tcIgnoreDirErr, "--ignore-file-errors", tcIgnoreFileErr)
					restoreDir := fmt.Sprintf("%s%d_%v_%v", restoreDirPrefix, tcIdx, tcIgnoreDirErr, tcIgnoreFileErr)
					testPermissions(e, t, snapSource, modifyEntry, restoreDir, tc.expectSuccess)

					e.RunAndExpectSuccess(t, "policy", "remove", snapSource)
				})
			}
		}
	}
}
func createSimplestFileTree(t *testing.T, dirDepth, currDepth int, currPath string) {
	dirname := fmt.Sprintf("dir%d", currDepth)
	dirPath := filepath.Join(currPath, dirname)
	err := os.MkdirAll(dirPath, 0700)
	testenv.AssertNoError(t, err)

	// Put an empty directory in the new directory
	emptyDirName := fmt.Sprintf("emptyDir%v", currDepth+1)
	emptyDirPath := filepath.Join(dirPath, emptyDirName)
	err = os.MkdirAll(emptyDirPath, 0700)
	testenv.AssertNoError(t, err)

	// Put a file in the new directory
	fileName := fmt.Sprintf("file%d", currDepth+1)
	filePath := filepath.Join(dirPath, fileName)

	testenv.MustCreateRandomFile(t, filePath, testenv.DirectoryTreeOptions{}, nil)

	if dirDepth > currDepth+1 {
		createSimplestFileTree(t, dirDepth, currDepth+1, dirPath)
	}
}

// testPermissions iterates over readable and executable permission states, testing
// files and directories (if present). It issues the kopia snapshot command
// against "source" and will test permissions against all entries in "parentDir".
// It returns the number of successful snapshot operations.
func testPermissions(e *testenv.CLITest, t *testing.T, source, modifyEntry, restoreDir string, expectSuccess map[os.FileMode]bool) int {
	t.Helper()

	var numSuccessfulSnapshots int

	changeFile, err := os.Stat(modifyEntry)
	testenv.AssertNoError(t, err)

	// Iterate over all permission bit configurations
	for chmod, shouldSucceed := range expectSuccess {
		mode := changeFile.Mode()
		t.Logf("Chmod: path: %s, isDir: %v, prevMode: %v, newMode: %v", modifyEntry, changeFile.IsDir(), mode, chmod)

		err := os.Chmod(modifyEntry, chmod)
		testenv.AssertNoError(t, err)

		if shouldSucceed {
			// Currently by default, the uploader has IgnoreFileErrors set to true.
			// Expect warning and successful snapshot creation
			_, errOut := e.RunAndExpectSuccessWithErrOut(t, "snapshot", "create", source)
			snapID := parseSnapID(t, errOut)
			numSuccessfulSnapshots++

			// Expect that since the snapshot succeeded, the data can be restored
			e.RunAndExpectSuccess(t, "snapshot", "restore", snapID, restoreDir)
		} else {
			e.RunAndExpectFailure(t, "snapshot", "create", source)
		}

		// Change permissions back and expect success
		err = os.Chmod(modifyEntry, mode.Perm())
		testenv.AssertNoError(t, err)
		e.RunAndExpectSuccess(t, "snapshot", "create", source)
		numSuccessfulSnapshots++
	}

	return numSuccessfulSnapshots
}

func parseSnapID(t *testing.T, lines []string) string {
	pattern := regexp.MustCompile(`Created snapshot with root [\S]+ and ID ([\S]+) in .*`)

	for _, l := range lines {
		match := pattern.FindAllStringSubmatch(l, 1)
		if len(match) > 0 && len(match[0]) > 1 {
			return match[0][1]
		}
	}

	t.Fatal("Snap ID could not be parsed")

	return ""
}
