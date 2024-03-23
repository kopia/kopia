package endtoend_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/tests/testdirtree"
	"github.com/kopia/kopia/tests/testenv"
)

func TestSnapshotNonexistent(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	scratchDir := testutil.TempDirectory(t)

	// Test snapshot of nonexistent directory fails
	e.RunAndExpectFailure(t, "snapshot", "create", filepath.Join(scratchDir, "notExist"))
}

func TestSnapshotFail_Default(t *testing.T) {
	t.Parallel()
	testSnapshotFailText(t, false, nil, nil)
}

func TestSnapshotFail_DefaultJSONOutput(t *testing.T) {
	t.Parallel()
	testSnapshotFail(t, false, []string{"--json"}, nil, parseSnapshotResultJSON)
}

func TestSnapshotFail_EnvOverride(t *testing.T) {
	t.Parallel()
	testSnapshotFailText(t, true, nil, map[string]string{"KOPIA_SNAPSHOT_FAIL_FAST": "true"})
}

func TestSnapshotFail_NoFailFast(t *testing.T) {
	t.Parallel()
	testSnapshotFailText(t, false, []string{"--no-fail-fast"}, nil)
}

func TestSnapshotFail_FailFast(t *testing.T) {
	t.Parallel()
	testSnapshotFailText(t, true, []string{"--fail-fast"}, nil)
}

type expectedSnapshotResult struct {
	success           bool
	wantErrors        int
	wantIgnoredErrors int
	wantPartial       bool
}

func cond(c bool, a, b int) int {
	if c {
		return a
	}

	return b
}

func testSnapshotFailText(t *testing.T, isFailFast bool, snapshotCreateFlags []string, snapshotCreateEnv map[string]string) {
	t.Helper()

	testSnapshotFail(t, isFailFast, snapshotCreateFlags, snapshotCreateEnv, parseSnapshotResultFromLog)
}

//nolint:thelper,cyclop
func testSnapshotFail(
	t *testing.T,
	isFailFast bool,
	snapshotCreateFlags []string,
	snapshotCreateEnv map[string]string,
	parseSnapshotResultFn func(t *testing.T, stdOut, _ []string) parsedSnapshotResult,
) {
	if runtime.GOOS == windowsOSName {
		t.Skip("this test does not work on Windows")
	}

	if os.Getuid() == 0 {
		t.Skip("this test does not work as root, because we're unable to remove permissions.")
	}

	const dir0Path = "dir0"

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

			var (
				expectedSuccess           = expectedSnapshotResult{success: true}
				expectEarlyFailure        = expectedSnapshotResult{success: false}
				expectedWhenIgnoringFiles = expectedSnapshotResult{success: ignoringFiles, wantErrors: cond(ignoringFiles, 0, 1), wantIgnoredErrors: cond(ignoringFiles, 1, 0), wantPartial: !ignoringFiles && isFailFast}
				expectedWhenIgnoringDirs  = expectedSnapshotResult{success: ignoringDirs, wantErrors: cond(ignoringDirs, 0, 1), wantIgnoredErrors: cond(ignoringDirs, 1, 0), wantPartial: !ignoringDirs && isFailFast}
			)

			// Test the root dir permissions
			for tcIdx, tc := range []struct {
				desc          string
				modifyEntry   string
				snapSource    string
				expectSuccess map[os.FileMode]expectedSnapshotResult
			}{
				{
					desc:        "Modify permissions of the parent dir of the snapshot source (source is a FILE)",
					modifyEntry: dir0Path,
					snapSource:  filepath.Join(dir0Path, "file1"),
					expectSuccess: map[os.FileMode]expectedSnapshotResult{
						0o000: expectEarlyFailure, // --- permission: cannot read directory
						0o100: expectedSuccess,    // --X permission: can enter directory and take snapshot of the file (with full permissions)
						0o400: expectEarlyFailure, // R-- permission: can read the file name, but will be unable to snapshot it without entering directory
					},
				},
				{
					desc:        "Modify permissions of the parent dir of the snapshot source (source is a DIRECTORY)",
					modifyEntry: dir0Path,
					snapSource:  filepath.Join(dir0Path, "dir1"),
					expectSuccess: map[os.FileMode]expectedSnapshotResult{
						0o000: expectEarlyFailure,
						0o100: expectedSuccess,
						0o400: expectEarlyFailure,
					},
				},
				{
					desc:        "Modify permissions of the parent dir of the snapshot source (source is an EMPTY directory)",
					modifyEntry: dir0Path,
					snapSource:  filepath.Join(dir0Path, "emptyDir1"),
					expectSuccess: map[os.FileMode]expectedSnapshotResult{
						0o000: expectEarlyFailure,
						0o100: expectedSuccess,
						0o400: expectEarlyFailure,
					},
				},
				{
					desc:        "Modify permissions of the snapshot source itself (source is a FILE)",
					modifyEntry: filepath.Join(dir0Path, "file1"),
					snapSource:  filepath.Join(dir0Path, "file1"),
					expectSuccess: map[os.FileMode]expectedSnapshotResult{
						0o000: expectEarlyFailure,
						0o100: expectEarlyFailure,
						0o400: expectedSuccess,
					},
				},
				{
					desc:        "Modify permissions of the snapshot source itself (source is a DIRECTORY)",
					modifyEntry: filepath.Join(dir0Path, "dir1"),
					snapSource:  filepath.Join(dir0Path, "dir1"),
					expectSuccess: map[os.FileMode]expectedSnapshotResult{
						0o000: expectEarlyFailure,
						0o100: expectEarlyFailure,
						0o400: expectEarlyFailure,
					},
				},
				{
					desc:        "Modify permissions of the snapshot source itself (source is an EMPTY directory)",
					modifyEntry: filepath.Join(dir0Path, "emptyDir1"),
					snapSource:  filepath.Join(dir0Path, "emptyDir1"),
					expectSuccess: map[os.FileMode]expectedSnapshotResult{
						0o000: expectEarlyFailure,
						0o100: expectEarlyFailure,
						0o400: expectedSuccess,
					},
				},
				{
					desc:        "Modify permissions of a FILE in the snapshot directory",
					modifyEntry: filepath.Join(dir0Path, "dir1", "file2"),
					snapSource:  filepath.Join(dir0Path, "dir1"),
					expectSuccess: map[os.FileMode]expectedSnapshotResult{
						0o000: expectedWhenIgnoringFiles,
						0o100: expectedWhenIgnoringFiles,
						0o400: expectedSuccess,
					},
				},
				{
					desc:        "Modify permissions of a DIRECTORY in the snapshot directory",
					modifyEntry: filepath.Join(dir0Path, "dir1", "dir2"),
					snapSource:  filepath.Join(dir0Path, "dir1"),
					expectSuccess: map[os.FileMode]expectedSnapshotResult{
						0o000: expectedWhenIgnoringDirs,
						0o100: expectedWhenIgnoringDirs,
						0o400: expectedWhenIgnoringDirs,
					},
				},
				{
					desc:        "Modify permissions of an EMPTY directory in the snapshot directory",
					modifyEntry: filepath.Join(dir0Path, "dir1", "emptyDir2"),
					snapSource:  filepath.Join(dir0Path, "dir1"),
					expectSuccess: map[os.FileMode]expectedSnapshotResult{
						0o000: expectedWhenIgnoringDirs,
						0o100: expectedWhenIgnoringDirs,
						0o400: expectedSuccess,
					},
				},
				{
					desc:        "Modify permissions of a FILE in a subdirectory of the snapshot root directory",
					modifyEntry: filepath.Join(dir0Path, "dir1", "file2"),
					snapSource:  dir0Path,
					expectSuccess: map[os.FileMode]expectedSnapshotResult{
						0o000: expectedWhenIgnoringFiles,
						0o100: expectedWhenIgnoringFiles,
						0o400: expectedSuccess,
					},
				},
				{
					desc:        "Modify permissions of a DIRECTORY in a subdirectory of the snapshot root directory",
					modifyEntry: filepath.Join(dir0Path, "dir1", "dir2"),
					snapSource:  dir0Path,
					expectSuccess: map[os.FileMode]expectedSnapshotResult{
						0o000: expectedWhenIgnoringDirs,
						0o100: expectedWhenIgnoringDirs,
						0o400: expectedWhenIgnoringDirs,
					},
				},
				{
					desc:        "Modify permissions of an EMPTY directory in a subdirectory of the snapshot root directory",
					modifyEntry: filepath.Join(dir0Path, "dir1", "emptyDir2"),
					snapSource:  dir0Path,
					expectSuccess: map[os.FileMode]expectedSnapshotResult{
						0o000: expectedWhenIgnoringDirs,
						0o100: expectedWhenIgnoringDirs,
						0o400: expectedSuccess,
					},
				},
			} {
				// Reference test conditions outside of range variables to satisfy linter
				tcIgnoreDirErr := ignoreDirErr
				tcIgnoreFileErr := ignoreFileErr
				tname := fmt.Sprintf("%s_ignoreFileErr_%s_ignoreDirErr_%s_failFast_%v", tc.desc, ignoreDirErr, ignoreFileErr, isFailFast)

				t.Run(tname, func(t *testing.T) {
					t.Parallel()

					runner := testenv.NewInProcRunner(t)
					e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

					defer e.RunAndExpectSuccess(t, "repo", "disconnect")

					e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

					scratchDir := testutil.TempDirectory(t)

					snapSource := filepath.Join(scratchDir, tc.snapSource)
					modifyEntry := filepath.Join(scratchDir, tc.modifyEntry)

					// Each directory tier will have a file, an empty directory, and the next tier's directory
					// (unless at max depth). Naming scheme is [file|dir|emptyDir][tier #].
					createSimplestFileTree(t, 3, 0, scratchDir)

					restoreDirPrefix := filepath.Join(scratchDir, "target")

					e.RunAndExpectSuccess(t, "policy", "set", snapSource, "--ignore-dir-errors", tcIgnoreDirErr, "--ignore-file-errors", tcIgnoreFileErr)
					restoreDir := fmt.Sprintf("%s%d_%v_%v", restoreDirPrefix, tcIdx, tcIgnoreDirErr, tcIgnoreFileErr)
					testPermissions(t, e, snapSource, modifyEntry, restoreDir, tc.expectSuccess, snapshotCreateFlags, snapshotCreateEnv, parseSnapshotResultFn)

					e.RunAndExpectSuccess(t, "policy", "remove", snapSource)
				})
			}
		}
	}
}

func createSimplestFileTree(t *testing.T, dirDepth, currDepth int, currPath string) {
	t.Helper()

	dirname := fmt.Sprintf("dir%d", currDepth)
	dirPath := filepath.Join(currPath, dirname)
	err := os.MkdirAll(dirPath, 0o700)
	require.NoError(t, err)

	// Put an empty directory in the new directory
	emptyDirName := fmt.Sprintf("emptyDir%v", currDepth+1)
	emptyDirPath := filepath.Join(dirPath, emptyDirName)
	err = os.MkdirAll(emptyDirPath, 0o700)
	require.NoError(t, err)

	// Put a file in the new directory
	fileName := fmt.Sprintf("file%d", currDepth+1)
	filePath := filepath.Join(dirPath, fileName)

	testdirtree.MustCreateRandomFile(t, filePath, testdirtree.DirectoryTreeOptions{}, nil)

	if dirDepth > currDepth+1 {
		createSimplestFileTree(t, dirDepth, currDepth+1, dirPath)
	}
}

// testPermissions iterates over readable and executable permission states, testing
// files and directories (if present). It issues the kopia snapshot command
// against "source" and will test permissions against all entries in "parentDir".
// It returns the number of successful snapshot operations.
//
//nolint:thelper
func testPermissions(
	t *testing.T,
	e *testenv.CLITest,
	source, modifyEntry, restoreDir string,
	expect map[os.FileMode]expectedSnapshotResult,
	snapshotCreateFlags []string,
	snapshotCreateEnv map[string]string,
	parseSnapshotResultFn func(_ *testing.T, _, _ []string) parsedSnapshotResult,
) int {
	var numSuccessfulSnapshots int

	changeFile, err := os.Stat(modifyEntry)
	require.NoError(t, err)

	// Iterate over all permission bit configurations
	for chmod, expected := range expect {
		// run in nested function go be able to do defer
		func() {
			mode := changeFile.Mode()

			// restore permissions even if we fail to avoid leaving non-deletable files behind.
			defer func() {
				t.Logf("restoring file mode on %s to %v", modifyEntry, mode)
				require.NoError(t, os.Chmod(modifyEntry, mode.Perm()))
			}()

			t.Logf("Chmod: path: %s, isDir: %v, prevMode: %v, newMode: %v", modifyEntry, changeFile.IsDir(), mode, chmod)

			err := os.Chmod(modifyEntry, chmod)
			require.NoError(t, err)

			// set up environment for the child process.
			oldEnv := e.Environment

			e.Environment = map[string]string{}
			for k, v := range oldEnv {
				e.Environment[k] = v
			}

			for k, v := range snapshotCreateEnv {
				e.Environment[k] = v
			}

			defer func() { e.Environment = oldEnv }()

			snapshotCreateWithArgs := append([]string{"snapshot", "create", source}, snapshotCreateFlags...)

			stdOut, stdErr, runErr := e.Run(t, !expected.success, snapshotCreateWithArgs...)

			if got, want := (runErr == nil), expected.success; got != want {
				t.Fatalf("unexpected success %v, want %v", got, want)
			}

			parsed := parseSnapshotResultFn(t, stdOut, stdErr)

			if expected.success {
				numSuccessfulSnapshots++

				e.RunAndExpectSuccess(t, "snapshot", "restore", parsed.manifestID, restoreDir)
			}

			if got, want := parsed.errorCount, expected.wantErrors; got != want {
				t.Fatalf("unexpected number of errors: %v, want %v", got, want)
			}

			if got, want := parsed.ignoredErrorCount, expected.wantIgnoredErrors; got != want {
				t.Fatalf("unexpected number of ignored errors: %v, want %v", got, want)
			}

			if got, want := parsed.partial, expected.wantPartial; got != want {
				t.Fatalf("unexpected partial %v, want %v (%s)", got, want, stdErr)
			}
		}()
	}

	return numSuccessfulSnapshots
}

var (
	createdSnapshotPattern = regexp.MustCompile(`Created (.*)snapshot with root (\S+) and ID (\S+) in .*`)
	fatalErrorsPattern     = regexp.MustCompile(`Found (\d+) fatal error\(s\) while snapshotting`)
	ignoredErrorsPattern   = regexp.MustCompile(`Ignored (\d+) error\(s\) while snapshotting`)
)

type parsedSnapshotResult struct {
	partial           bool
	rootID            string
	manifestID        string
	errorCount        int
	ignoredErrorCount int
}

func parseSnapshotResultFromLog(t *testing.T, _, stdErr []string) parsedSnapshotResult {
	t.Helper()

	var (
		err error
		res parsedSnapshotResult
	)

	for _, l := range stdErr {
		if match := createdSnapshotPattern.FindStringSubmatch(l); match != nil {
			res.partial = strings.TrimSpace(match[1]) == "partial"
			res.rootID = match[2]
			res.manifestID = match[3]
		}

		if match := fatalErrorsPattern.FindStringSubmatch(l); match != nil {
			res.errorCount, err = strconv.Atoi(match[1])
			if err != nil {
				t.Fatal(err)
			}
		}

		if match := ignoredErrorsPattern.FindStringSubmatch(l); match != nil {
			res.ignoredErrorCount, err = strconv.Atoi(match[1])
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	return res
}

func parseSnapshotResultJSON(t *testing.T, stdOut, _ []string) parsedSnapshotResult {
	t.Helper()

	if len(stdOut) == 0 {
		return parsedSnapshotResult{}
	}

	var m snapshot.Manifest

	testutil.MustParseJSONLines(t, stdOut, &m)

	return parsedSnapshotResult{
		manifestID:        string(m.ID),
		rootID:            m.RootEntry.ObjectID.String(),
		errorCount:        m.RootEntry.DirSummary.FatalErrorCount,
		ignoredErrorCount: m.RootEntry.DirSummary.IgnoredErrorCount,
		partial:           m.IncompleteReason != "",
	}
}
