package endtoend_test

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/cli"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/diff"
	"github.com/kopia/kopia/internal/fshasher"
	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/stat"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/tests/clitestutil"
	"github.com/kopia/kopia/tests/testdirtree"
	"github.com/kopia/kopia/tests/testenv"
)

const (
	windowsOSName                 = "windows"
	defaultRestoredFilePermission = 0o600

	overriddenFilePermissions = 0o651
	overriddenDirPermissions  = 0o752
)

type fakeRestoreProgress struct {
	mtx                  sync.Mutex
	invocations          []restore.Stats
	flushesCount         int
	invocationAfterFlush bool
}

func (p *fakeRestoreProgress) SetCounters(s restore.Stats) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.invocations = append(p.invocations, s)

	if p.flushesCount > 0 {
		p.invocationAfterFlush = true
	}
}

func (p *fakeRestoreProgress) Flush() {
	p.flushesCount++
}

func TestRestoreCommand(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := filepath.Join(testutil.TempDirectory(t), "source")
	testdirtree.MustCreateDirectoryTree(t, source, testdirtree.DirectoryTreeOptions{
		Depth:                              1,
		MaxFilesPerDirectory:               10,
		MaxSymlinksPerDirectory:            4,
		NonExistingSymlinkTargetPercentage: 50,
	})

	r1 := testutil.TempDirectory(t)
	// Attempt to restore a snapshot from an empty repo.
	e.RunAndExpectFailure(t, "restore", "kffbb7c28ea6c34d6cbe555d1cf80faa9", r1)
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

	r2 := testutil.TempDirectory(t)
	// Attempt to restore a non-existing snapshot.
	e.RunAndExpectFailure(t, "restore", "kffbb7c28ea6c34d6cbe555d1cf80fdd9", r2)

	// Ensure restored files are created with a different ModTime
	time.Sleep(time.Second)

	// Attempt to restore using snapshot ID
	restoreFailDir := testutil.TempDirectory(t)

	// Remember original app cusomization
	origCustomizeApp := runner.CustomizeApp

	// Prepare fake restore progress and set it when needed
	frp := &fakeRestoreProgress{}

	runner.CustomizeApp = func(a *cli.App, kp *kingpin.Application) {
		origCustomizeApp(a, kp)
		a.SetRestoreProgress(frp)
	}

	e.RunAndExpectSuccess(t, "restore", snapID, restoreFailDir, "--progress-update-interval", "1ms")

	runner.CustomizeApp = origCustomizeApp

	// Expecting progress to be reported multiple times and flush to be invoked at the end
	require.Greater(t, len(frp.invocations), 2, "expected multiple reports of progress")
	require.Equal(t, 1, frp.flushesCount, "expected to have progress flushed once")
	require.False(t, frp.invocationAfterFlush, "expected not to have reports after flush")

	// Restore last snapshot
	restoreDir := testutil.TempDirectory(t)
	e.RunAndExpectSuccess(t, "restore", rootID, restoreDir)

	// Note: restore does not reset the permissions for the top directory due to
	// the way the top FS entry is created in snapshotfs. Force the permissions
	// of the top directory to match those of the source so the recursive
	// directory comparison has a chance of succeeding.
	require.NoError(t, os.Chmod(restoreDir, 0o700))
	compareDirs(t, source, restoreDir)

	// Attempt to restore into a target directory that already exists
	e.RunAndExpectFailure(t, "restore", rootID, restoreDir, "--no-overwrite-directories")

	// Very quick incremental restore where all files already exist.
	// Look for status output that indicates files were skipped.
	re := regexp.MustCompile(`Restored (\d+) files.*skipped (\d+) `)
	foundStatus := false
	lastFileCount := 0
	_, stderr := e.RunAndExpectSuccessWithErrOut(t, "restore", rootID, restoreDir, "--skip-existing")

	for _, l := range stderr {
		if m := re.FindStringSubmatch(l); m != nil {
			fileCount, _ := strconv.Atoi(m[1])
			skippedCount, _ := strconv.Atoi(m[2])
			lastFileCount = fileCount

			if fileCount == 0 && skippedCount > 0 {
				foundStatus = true
			}
		}
	}

	if !foundStatus {
		t.Fatalf("expected status line indicating files were skipped, none found: %v", stderr)
	}

	if lastFileCount != 0 {
		t.Fatalf("not all files were skipped: %v", stderr)
	}

	// Attempt to restore into a target directory that already exists
	e.RunAndExpectFailure(t, "restore", rootID, restoreDir, "--no-overwrite-files")
}

func compareDirs(t *testing.T, source, restoreDir string) {
	t.Helper()

	// Restored contents should match source
	s, err := localfs.Directory(source)
	require.NoError(t, err)
	wantHash, err := fshasher.Hash(testlogging.Context(t), s)
	require.NoError(t, err)

	// check restored contents
	r, err := localfs.Directory(restoreDir)
	require.NoError(t, err)

	ctx := testlogging.Context(t)
	gotHash, err := fshasher.Hash(ctx, r)
	require.NoError(t, err)

	if !assert.Equal(t, wantHash, gotHash, "restored directory hash does not match source's hash") {
		cmp, err := diff.NewComparer(os.Stderr)
		require.NoError(t, err)

		cmp.DiffCommand = "cmp"
		_ = cmp.Compare(ctx, s, r)
	}
}

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := testutil.TempDirectory(t)
	testdirtree.MustCreateDirectoryTree(t, filepath.Join(source, "subdir1"), testdirtree.MaybeSimplifyFilesystem(testdirtree.DirectoryTreeOptions{
		Depth:                              3,
		MaxSubdirsPerDirectory:             3,
		MaxFilesPerDirectory:               3,
		MaxSymlinksPerDirectory:            4,
		NonExistingSymlinkTargetPercentage: 50,
	}))
	testdirtree.MustCreateDirectoryTree(t, filepath.Join(source, "subdir2"), testdirtree.MaybeSimplifyFilesystem(testdirtree.DirectoryTreeOptions{
		Depth:                   2,
		MaxSubdirsPerDirectory:  1,
		MaxFilesPerDirectory:    5,
		MaxSymlinksPerDirectory: 4,
	}))

	// create a file with well-known name.
	f, err := os.Create(filepath.Join(source, "single-file"))
	require.NoError(t, err)

	fmt.Fprintf(f, "some-data")
	f.Close()

	// change file permissions to something unique we can test later
	os.Chmod(filepath.Join(source, "single-file"), overriddenFilePermissions)
	os.Chmod(filepath.Join(source, "subdir1"), overriddenDirPermissions)

	restoreDir := testutil.TempDirectory(t)
	r1 := testutil.TempDirectory(t)
	// Attempt to restore a snapshot from an empty repo.
	e.RunAndExpectFailure(t, "snapshot", "restore", "kffbb7c28ea6c34d6cbe555d1cf80faa9", r1)
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

	// Attempt to restore a non-existing snapshot.
	r2 := testutil.TempDirectory(t)
	e.RunAndExpectFailure(t, "snapshot", "restore", "kffbb7c28ea6c34d6cbe555d1cf80fdd9", r2)

	// Ensure restored files are created with a different ModTime
	time.Sleep(time.Second)

	// Attempt to restore snapshot with root ID
	restoreByObjectIDDir := testutil.TempDirectory(t)
	e.RunAndExpectSuccess(t, "snapshot", "restore", rootID, restoreByObjectIDDir)

	// restore using <root-id>/subdirectory.
	restoreByOIDSubdir := testutil.TempDirectory(t)
	e.RunAndExpectSuccess(t, "snapshot", "restore", rootID+"/subdir1", restoreByOIDSubdir)
	verifyFileMode(t, restoreByOIDSubdir, os.ModeDir|os.FileMode(overriddenDirPermissions))

	restoreByOIDSubdir2 := testutil.TempDirectory(t)

	originalDirInfo, err := os.Stat(restoreByOIDSubdir2)
	if err != nil {
		t.Fatalf("unable to get dir permissions: %v", err)
	}

	e.RunAndExpectSuccess(t, "snapshot", "restore", "--skip-times", "--skip-owners", "--skip-permissions", rootID+"/subdir1", restoreByOIDSubdir2)

	currentDirPerm, err := os.Stat(restoreByOIDSubdir2)
	if err != nil {
		t.Fatalf("unable to get current dir permissions: %v", err)
	}

	if currentDirPerm.Mode() != originalDirInfo.Mode() {
		t.Fatalf("dir mode have changed, original %v, current %v", originalDirInfo.Mode(), currentDirPerm.Mode())
	}

	// current must be always at or after original, if it's not it must have been restored.
	if currentDirPerm.ModTime().Before(originalDirInfo.ModTime()) {
		t.Fatalf("dir ModTime has been restore, original %v, current %v", originalDirInfo.ModTime(), currentDirPerm.ModTime())
	}

	// TODO(jkowalski): find a way to verify owners, we currently cannot even change it since the test is running as
	// non-root.

	restoreByOIDFile := testutil.TempDirectory(t)

	// restoring single file onto a directory fails
	e.RunAndExpectFailure(t, "snapshot", "restore", rootID+"/single-file", restoreByOIDFile)

	// restoring single file
	e.RunAndExpectSuccess(t, "snapshot", "restore", rootID+"/single-file", filepath.Join(restoreByOIDFile, "output-file"))
	verifyFileMode(t, filepath.Join(restoreByOIDFile, "output-file"), overriddenFilePermissions)

	// Restore last snapshot using the snapshot ID
	e.RunAndExpectSuccess(t, "snapshot", "restore", snapID, restoreDir)

	// Restored contents should match source
	compareDirs(t, source, restoreDir)

	// Check restore idempotency. Repeat the restore into the already-restored directory.
	// If running the test as non-admin on Windows, there may not be sufficient permissions
	// to overwrite the existing files, so skip this check to avoid "Access is denied" errors.
	if runtime.GOOS != windowsOSName {
		e.RunAndExpectSuccess(t, "snapshot", "restore", snapID, restoreDir)
	}

	// Restored contents should still match source
	compareDirs(t, source, restoreDir)

	cases := []struct {
		fname     string
		args      []string
		validator func(t *testing.T, fname string)
	}{
		// auto-detected formats
		{fname: "output.zip", args: nil, validator: verifyValidZipFile},
		{fname: "output.tar", args: nil, validator: verifyValidTarFile},
		{fname: "output.tar.gz", args: nil, validator: verifyValidTarGzipFile},
		{fname: "output.tgz", args: nil, validator: verifyValidTarGzipFile},
		// forced formats
		{fname: "output.nonzip.blah", args: []string{"--mode=zip"}, validator: verifyValidZipFile},
		{fname: "output.nontar.blah", args: []string{"--mode=tar"}, validator: verifyValidTarFile},
		{fname: "output.notargz.blah", args: []string{"--mode=tgz"}, validator: verifyValidTarGzipFile},
	}

	restoreArchiveDir := testutil.TempDirectory(t)

	t.Run("modes", func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.fname, func(t *testing.T) {
				t.Parallel()
				fname := filepath.Join(restoreArchiveDir, tc.fname)
				e.RunAndExpectSuccess(t, append([]string{"snapshot", "restore", snapID, fname}, tc.args...)...)
				tc.validator(t, fname)
			})
		}
	})

	// create a directory whose name ends with '.zip' and override mode to force treating it as directory.
	zipDir := filepath.Join(restoreArchiveDir, "outputdir.zip")
	e.RunAndExpectSuccess(t, "snapshot", "restore", snapID, zipDir, "--mode=local")

	// verify we got a directory
	st, err := os.Stat(zipDir)
	if err != nil || !st.IsDir() {
		t.Fatalf("unexpected stat() results on output.zip directory %v %v", st, err)
	}

	// Attempt to restore snapshot with an already-existing target directory
	// It should fail because the directory is not empty
	e.RunAndExpectFailure(t, "snapshot", "restore", "--no-overwrite-directories", snapID, restoreDir)

	// Attempt to restore snapshot with an already-existing target directory
	// It should fail because target files already exist
	e.RunAndExpectFailure(t, "snapshot", "restore", "--no-overwrite-files", snapID, restoreDir)

	// Attempt to restore snapshot with an already-existing target directory
	// It should fail because target symlinks already exist
	e.RunAndExpectFailure(t, "snapshot", "restore", "--no-overwrite-symlinks", snapID, restoreDir)
}

func TestRestoreSymlinkWithoutTarget(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := testutil.TempDirectory(t)

	lnk := filepath.Join(source, "lnk")

	if err := os.Symlink(".no-such-file", lnk); err != nil {
		t.Fatal(err)
	}

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

	restoredDir := testutil.TempDirectory(t)
	e.RunAndExpectSuccess(t, "snapshot", "restore", "--no-ignore-permission-errors", snapID, restoredDir)
}

func TestRestoreSymlinkWithNonSymlinkOverwrite(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := testutil.TempDirectory(t)

	testLinkName := "lnk"
	lnkPath := filepath.Join(source, testLinkName)

	if err := os.Symlink(".no-such-file", lnkPath); err != nil {
		t.Fatal(err)
	}

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

	restoreDir := testutil.TempDirectory(t)

	// Make a directory containing a non-symlink entry named the same as the link captured by the snapshot
	dirWithLinkName := filepath.Join(restoreDir, testLinkName)

	if err := os.Mkdir(dirWithLinkName, 0o644); err != nil {
		t.Fatal(err)
	}

	e.RunAndExpectFailure(t, "snapshot", "restore", snapID, restoreDir)
}

func TestRestoreSnapshotOfSingleFile(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	sourceDir := testutil.TempDirectory(t)
	sourceFile := filepath.Join(sourceDir, "single-file")

	f, err := os.Create(sourceFile)
	require.NoError(t, err)

	fmt.Fprintf(f, "some-data")
	f.Close()

	// set up unique file mode which will be verified later.
	os.Chmod(sourceFile, 0o653)

	e.RunAndExpectSuccess(t, "snapshot", "create", sourceFile)

	// obtain snapshot root id and use it for restore
	si := clitestutil.ListSnapshotsAndExpectSuccess(t, e, sourceFile)
	if got, want := len(si), 1; got != want {
		t.Fatalf("got %v sources, wanted %v", got, want)
	}

	if got, want := len(si[0].Snapshots), 1; got != want {
		t.Fatalf("got %v snapshots, wanted %v", got, want)
	}

	snapID := si[0].Snapshots[0].SnapshotID
	rootID := si[0].Snapshots[0].ObjectID

	restoreDir := testutil.TempDirectory(t)

	// restoring a file to a directory destination fails.
	e.RunAndExpectFailure(t, "snapshot", "restore", snapID, restoreDir)

	e.RunAndExpectSuccess(t, "snapshot", "restore", snapID, filepath.Join(restoreDir, "restored-1"))
	verifyFileMode(t, filepath.Join(restoreDir, "restored-1"), os.FileMode(0o653))

	// we can restore using rootID because it's unambiguous
	e.RunAndExpectSuccess(t, "snapshot", "restore", rootID, filepath.Join(restoreDir, "restored-2"))
	verifyFileMode(t, filepath.Join(restoreDir, "restored-2"), os.FileMode(0o653))

	if runtime.GOOS != windowsOSName {
		overriddenFilePermissions := 0o654 | os.ModeSetuid

		// change source file permissions and create one more snapshot
		// at this point we will have multiple snapshot manifests with one root but different attributes.
		os.Chmod(sourceFile, overriddenFilePermissions)
		e.RunAndExpectSuccess(t, "snapshot", "create", sourceFile)

		// when restoring by root Kopia needs to pick which manifest to apply since they are conflicting
		// We're passing --consistent-attributes which causes it to fail, since otherwise we'd restore arbitrary
		// top-level object permissions.
		e.RunAndExpectFailure(t, "snapshot", "restore", rootID, "--consistent-attributes", filepath.Join(restoreDir, "restored-3"))

		// Without the flag kopia picks the attributes from the latest snapshot.
		e.RunAndExpectSuccess(t, "snapshot", "restore", rootID, filepath.Join(restoreDir, "restored-3"))
		verifyFileMode(t, filepath.Join(restoreDir, "restored-3"), overriddenFilePermissions)
	}

	// restoring using snapshot ID is unambiguous and always produces file with 0o653
	e.RunAndExpectSuccess(t, "snapshot", "restore", snapID, filepath.Join(restoreDir, "restored-4"))
	verifyFileMode(t, filepath.Join(restoreDir, "restored-4"), os.FileMode(0o653))

	// skip permissions when restoring, which results in default defaultRestoredFilePermission
	e.RunAndExpectSuccess(t, "snapshot", "restore", rootID, "--skip-permissions", filepath.Join(restoreDir, "restored-5"))

	verifyFileMode(t, filepath.Join(restoreDir, "restored-5"), defaultRestoredFilePermission)
}

func TestSnapshotSparseRestore(t *testing.T) {
	t.Parallel()

	// The behavior of the Darwin (APFS) is not published, and sparse restores
	// are not supported on Windows. As such, we cannot (reliably) test them here.
	testutil.TestSkipUnlessLinux(t)

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	sourceDir := testutil.TempDirectory(t)
	restoreDir := testutil.TempDirectory(t)

	bufSize := uint64(iocopy.BufSize)

	blkSize, err := stat.GetBlockSize(restoreDir)
	if err != nil {
		t.Fatalf("error getting disk block size: %v", err)
	}

	type chunk struct {
		slice []byte
		off   uint64
		rep   uint64
	}

	cases := []struct {
		name  string
		data  []chunk
		trunc uint64 // Truncate source file to this size
		sLog  uint64 // Expected logical size of source file
		sPhys uint64 // Expected physical size of source file
		rLog  uint64 // Expected logical size of restored file
		rPhys uint64 // Expected physical size of restored file
	}{
		{
			name:  "null_file",
			trunc: 0,
			sLog:  0,
			sPhys: 0,
			rLog:  0,
			rPhys: 0,
		},
		{
			name:  "empty_file",
			trunc: 3 * bufSize,
			sLog:  3 * bufSize,
			sPhys: 0,
			rLog:  3 * bufSize,
			rPhys: 0,
		},
		{
			name: "blk",
			data: []chunk{
				{slice: []byte("1"), off: 0, rep: blkSize},
			},
			sLog:  blkSize,
			sPhys: blkSize,
			rLog:  blkSize,
			rPhys: blkSize,
		},
		{
			name: "blk_real_zeros",
			data: []chunk{
				{slice: []byte{0}, off: 0, rep: blkSize},
			},
			sLog:  blkSize,
			sPhys: blkSize,
			rLog:  blkSize,
			rPhys: 0,
		},
		{
			name: "buf_real_zeros",
			data: []chunk{
				{slice: []byte{0}, off: 0, rep: bufSize},
			},
			sLog:  bufSize,
			sPhys: bufSize,
			rLog:  bufSize,
			rPhys: 0,
		},
		{
			name: "buf_full",
			data: []chunk{
				{slice: []byte("1"), off: 0, rep: bufSize},
			},
			sLog:  bufSize,
			sPhys: bufSize,
			rLog:  bufSize,
			rPhys: bufSize,
		},
		{
			name: "buf_trailing_bytes",
			data: []chunk{
				{slice: []byte("1"), off: bufSize - blkSize - 1, rep: 1},
				{slice: []byte("1"), off: bufSize - 1, rep: 1},
			},
			trunc: bufSize,
			sLog:  bufSize,
			sPhys: 2 * blkSize,
			rLog:  bufSize,
			rPhys: 2 * blkSize,
		},
		{
			name: "buf_trailing_hole",
			data: []chunk{
				{slice: []byte("1"), off: 0, rep: 1},
			},
			trunc: bufSize,
			sLog:  bufSize,
			sPhys: blkSize,
			rLog:  bufSize,
			rPhys: blkSize,
		},
		{
			name: "buf_hole_aligned",
			data: []chunk{
				{slice: []byte("1"), off: bufSize, rep: blkSize},
			},
			trunc: bufSize + blkSize,
			sLog:  bufSize + blkSize,
			sPhys: blkSize,
			rLog:  bufSize + blkSize,
			rPhys: blkSize,
		},
		{
			name: "buf_hole_on_buf_boundary",
			data: []chunk{
				{slice: []byte("1"), off: bufSize / 2, rep: bufSize},
			},
			sLog:  bufSize * 3 / 2,
			sPhys: bufSize,
			rLog:  bufSize * 3 / 2,
			rPhys: bufSize,
		},
		{
			name: "blk_hole_on_blk_boundary",
			data: []chunk{
				{slice: []byte("1"), off: blkSize / 2, rep: blkSize},
			},
			sLog:  blkSize * 3 / 2,
			sPhys: blkSize * 2,
			rLog:  blkSize * 3 / 2,
			rPhys: blkSize * 2,
		},
		{
			name: "blk_hole_on_buf_boundary",
			data: []chunk{
				{slice: []byte("1"), off: 0, rep: bufSize - (blkSize / 2)},
				{slice: []byte("1"), off: bufSize + (blkSize / 2), rep: blkSize / 2},
			},
			sLog:  bufSize + blkSize,
			sPhys: bufSize + blkSize,
			rLog:  bufSize + blkSize,
			rPhys: bufSize + blkSize,
		},
		{
			name: "blk_hole_aligned",
			data: []chunk{
				{slice: []byte("1"), off: 0, rep: bufSize},
				{slice: []byte("1"), off: bufSize + blkSize, rep: bufSize - blkSize},
			},
			trunc: 2 * bufSize,
			sLog:  2 * bufSize,
			sPhys: 2*bufSize - blkSize,
			rLog:  2 * bufSize,
			rPhys: 2*bufSize - blkSize,
		},
		{
			name: "blk_alternating_empty",
			data: []chunk{
				{slice: []byte("1"), off: 0, rep: blkSize},
				{slice: []byte("1"), off: 1 * blkSize, rep: blkSize},
				{slice: []byte("1"), off: 4 * blkSize, rep: blkSize},
				{slice: []byte("1"), off: 6 * blkSize, rep: blkSize},
				{slice: []byte("1"), off: 8 * blkSize, rep: blkSize},
			},
			sLog:  9 * blkSize,
			sPhys: 5 * blkSize,
			rLog:  9 * blkSize,
			rPhys: 5 * blkSize,
		},
		{
			name: "blk_alternating_zero",
			data: []chunk{
				{slice: []byte("1"), off: 0, rep: blkSize},
				{slice: []byte{0}, off: blkSize, rep: blkSize},
				{slice: []byte("1"), off: 2 * blkSize, rep: blkSize},
				{slice: []byte{0}, off: 3 * blkSize, rep: blkSize},
			},
			sLog:  4 * blkSize,
			sPhys: 4 * blkSize,
			rLog:  4 * blkSize,
			rPhys: 2 * blkSize,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.name == "blk_hole_on_buf_boundary" && runtime.GOARCH == "arm64" {
				t.Skip("skipping on arm64 due to a failure - https://github.com/kopia/kopia/issues/3178")
			}

			sourceFile := filepath.Join(sourceDir, c.name+"_source")

			fd, err := os.Create(sourceFile)
			require.NoError(t, err)

			err = fd.Truncate(int64(c.trunc))
			require.NoError(t, err)

			for _, d := range c.data {
				fd.WriteAt(bytes.Repeat(d.slice, int(d.rep)), int64(d.off))
			}

			verifyFileSize(t, sourceFile, c.sLog, c.sPhys)
			e.RunAndExpectSuccess(t, "snapshot", "create", sourceFile)

			si := clitestutil.ListSnapshotsAndExpectSuccess(t, e, sourceFile)
			require.Len(t, si, 1)
			require.Len(t, si[0].Snapshots, 1)

			snapID := si[0].Snapshots[0].SnapshotID
			restoreFile := filepath.Join(restoreDir, c.name+"_restore")

			e.RunAndExpectSuccess(t, "snapshot", "restore", snapID, "--write-sparse-files", restoreFile)
			verifyFileSize(t, restoreFile, c.rLog, c.rPhys)
		})
	}
}

func verifyFileSize(t *testing.T, fname string, logical, physical uint64) {
	t.Helper()

	st, err := os.Stat(fname)
	require.NoError(t, err)

	realLogical := uint64(st.Size())

	require.Equal(t, logical, realLogical)

	if runtime.GOOS == windowsOSName {
		t.Logf("getting physical file size is not supported on windows")
		return
	}

	realPhysical, err := stat.GetFileAllocSize(fname)
	require.NoError(t, err)

	require.Equal(t, physical, realPhysical)
}

func verifyFileMode(t *testing.T, filename string, want os.FileMode) {
	t.Helper()

	if runtime.GOOS == windowsOSName {
		// do not compare Unix filemodes on Windows.
		return
	}

	s, err := os.Lstat(filename)
	require.NoError(t, err)

	// make sure we restored permissions correctly
	if s.Mode() != want {
		t.Fatalf("invalid mode on %v: %v, want %v", filename, s.Mode(), want)
	}
}

func verifyValidZipFile(t *testing.T, fname string) {
	t.Helper()

	zr, err := zip.OpenReader(fname)
	require.NoError(t, err)

	defer zr.Close()
}

func verifyValidTarFile(t *testing.T, fname string) {
	t.Helper()

	f, err := os.Open(fname)
	require.NoError(t, err)

	defer f.Close()

	verifyValidTarReader(t, tar.NewReader(f))
}

func verifyValidTarReader(t *testing.T, tr *tar.Reader) {
	t.Helper()

	_, err := tr.Next()
	for err == nil {
		_, err = tr.Next()
	}

	if !errors.Is(err, io.EOF) {
		t.Fatalf("invalid tar file: %v", err)
	}
}

func verifyValidTarGzipFile(t *testing.T, fname string) {
	t.Helper()

	f, err := os.Open(fname)
	require.NoError(t, err)

	defer f.Close()

	gz, err := gzip.NewReader(f)
	require.NoError(t, err)

	verifyValidTarReader(t, tar.NewReader(gz))
}

func TestSnapshotRestoreByPath(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := testutil.TempDirectory(t)

	// create a file with well-known name.
	f, err := os.Create(filepath.Join(source, "single-file"))
	require.NoError(t, err)

	fmt.Fprintf(f, "some-data")
	f.Close()

	// Create snapshot
	e.RunAndExpectSuccess(t, "snapshot", "create", source)

	// Restore based on source path
	restoreDir := testutil.TempDirectory(t)
	e.RunAndExpectSuccess(t, "snapshot", "restore", source, restoreDir, "--snapshot-time=latest")

	// Restored contents should match source
	compareDirs(t, source, restoreDir)
}

func TestRestoreByPathWithoutTarget(t *testing.T) {
	t.Parallel()

	runner := testenv.NewInProcRunner(t)
	e := testenv.NewCLITest(t, testenv.RepoFormatNotImportant, runner)

	defer e.RunAndExpectSuccess(t, "repo", "disconnect")
	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	srcdir := testutil.TempDirectory(t)
	file := filepath.Join(srcdir, "a.txt")
	originalData := []byte{1, 2, 3}

	require.NoError(t, os.WriteFile(file, originalData, 0o755))

	e.RunAndExpectSuccess(t, "snapshot", "create", srcdir)

	require.NoError(t, os.WriteFile(file, []byte{4, 5, 6, 7}, 0o755))

	e.RunAndExpectSuccess(t, "restore", srcdir, "--snapshot-time=latest")

	data, err := os.ReadFile(file)

	require.NoError(t, err)
	require.Equal(t, originalData, data)

	// Defaults to latest snapshot time
	e.RunAndExpectSuccess(t, "restore", srcdir)
}
