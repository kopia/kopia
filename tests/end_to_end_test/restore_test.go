package endtoend_test

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/diff"
	"github.com/kopia/kopia/internal/fshasher"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/tests/testenv"
)

const (
	windowsOSName                 = "windows"
	defaultRestoredFilePermission = 0o600

	overriddenFilePermissions = 0o651
	overriddenDirPermissions  = 0o752
)

func TestRestoreCommand(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := filepath.Join(t.TempDir(), "source")
	testenv.MustCreateDirectoryTree(t, source, testenv.DirectoryTreeOptions{
		Depth:                              1,
		MaxFilesPerDirectory:               10,
		MaxSymlinksPerDirectory:            4,
		NonExistingSymlinkTargetPercentage: 50,
	})

	r1 := t.TempDir()
	// Attempt to restore a snapshot from an empty repo.
	e.RunAndExpectFailure(t, "restore", "kffbb7c28ea6c34d6cbe555d1cf80faa9", r1)
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

	r2 := t.TempDir()
	// Attempt to restore a non-existing snapshot.
	e.RunAndExpectFailure(t, "restore", "kffbb7c28ea6c34d6cbe555d1cf80fdd9", r2)

	// Ensure restored files are created with a different ModTime
	time.Sleep(time.Second)

	// Attempt to restore using snapshot ID
	restoreFailDir := t.TempDir()
	e.RunAndExpectSuccess(t, "restore", snapID, restoreFailDir)

	// Restore last snapshot
	restoreDir := t.TempDir()
	e.RunAndExpectSuccess(t, "restore", rootID, restoreDir)

	// Note: restore does not reset the permissions for the top directory due to
	// the way the top FS entry is created in snapshotfs. Force the permissions
	// of the top directory to match those of the source so the recursive
	// directory comparison has a chance of succeeding.
	testenv.AssertNoError(t, os.Chmod(restoreDir, 0o700))
	compareDirs(t, source, restoreDir)

	// Attempt to restore into a target directory that already exists
	e.RunAndExpectFailure(t, "restore", rootID, restoreDir, "--no-overwrite-directories")

	// Attempt to restore into a target directory that already exists
	e.RunAndExpectFailure(t, "restore", rootID, restoreDir, "--no-overwrite-files")
}

func compareDirs(t *testing.T, source, restoreDir string) {
	t.Helper()

	// Restored contents should match source
	s, err := localfs.Directory(source)
	testenv.AssertNoError(t, err)
	wantHash, err := fshasher.Hash(testlogging.Context(t), s)
	testenv.AssertNoError(t, err)

	// check restored contents
	r, err := localfs.Directory(restoreDir)
	testenv.AssertNoError(t, err)

	ctx := testlogging.Context(t)
	gotHash, err := fshasher.Hash(ctx, r)
	testenv.AssertNoError(t, err)

	if !assert.Equal(t, wantHash, gotHash, "restored directory hash does not match source's hash") {
		cmp, err := diff.NewComparer(os.Stderr)
		testenv.AssertNoError(t, err)

		cmp.DiffCommand = "cmp"
		_ = cmp.Compare(ctx, s, r)
	}
}

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := t.TempDir()
	testenv.MustCreateDirectoryTree(t, filepath.Join(source, "subdir1"), testenv.DirectoryTreeOptions{
		Depth:                              5,
		MaxSubdirsPerDirectory:             5,
		MaxFilesPerDirectory:               5,
		MaxSymlinksPerDirectory:            4,
		NonExistingSymlinkTargetPercentage: 50,
	})
	testenv.MustCreateDirectoryTree(t, filepath.Join(source, "subdir2"), testenv.DirectoryTreeOptions{
		Depth:                   2,
		MaxSubdirsPerDirectory:  1,
		MaxFilesPerDirectory:    5,
		MaxSymlinksPerDirectory: 4,
	})

	// create a file with well-known name.
	f, err := os.Create(filepath.Join(source, "single-file"))
	if err != nil {
		t.Fatal(err)
	}

	fmt.Fprintf(f, "some-data")
	f.Close()

	// change file permissions to something unique we can test later
	os.Chmod(filepath.Join(source, "single-file"), overriddenFilePermissions)
	os.Chmod(filepath.Join(source, "subdir1"), overriddenDirPermissions)

	restoreDir := t.TempDir()
	r1 := t.TempDir()
	// Attempt to restore a snapshot from an empty repo.
	e.RunAndExpectFailure(t, "snapshot", "restore", "kffbb7c28ea6c34d6cbe555d1cf80faa9", r1)
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

	// Attempt to restore a non-existing snapshot.
	r2 := t.TempDir()
	e.RunAndExpectFailure(t, "snapshot", "restore", "kffbb7c28ea6c34d6cbe555d1cf80fdd9", r2)

	// Ensure restored files are created with a different ModTime
	time.Sleep(time.Second)

	// Attempt to restore snapshot with root ID
	restoreByObjectIDDir := t.TempDir()
	e.RunAndExpectSuccess(t, "snapshot", "restore", rootID, restoreByObjectIDDir)

	// restore using <root-id>/subdirectory.
	restoreByOIDSubdir := t.TempDir()
	e.RunAndExpectSuccess(t, "snapshot", "restore", rootID+"/subdir1", restoreByOIDSubdir)
	verifyFileMode(t, restoreByOIDSubdir, os.ModeDir|os.FileMode(overriddenDirPermissions))

	restoreByOIDSubdir2 := t.TempDir()

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

	restoreByOIDFile := t.TempDir()

	// restoring single file onto a directory fails
	e.RunAndExpectFailure(t, "snapshot", "restore", rootID+"/single-file", restoreByOIDFile)

	// restoring single file
	e.RunAndExpectSuccess(t, "snapshot", "restore", rootID+"/single-file", filepath.Join(restoreByOIDFile, "output-file"))
	verifyFileMode(t, filepath.Join(restoreByOIDFile, "output-file"), os.FileMode(0o651))

	// Restore last snapshot using the snapshot ID
	e.RunAndExpectSuccess(t, "snapshot", "restore", snapID, restoreDir)

	// Restored contents should match source
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

	restoreArchiveDir := t.TempDir()

	t.Run("modes", func(t *testing.T) {
		for _, tc := range cases {
			tc := tc
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

	restoreFailDir := t.TempDir()

	// Attempt to restore snapshot with an already-existing target directory
	// It should fail because the directory is not empty
	_ = os.MkdirAll(restoreFailDir, 0o700)

	e.RunAndExpectFailure(t, "snapshot", "restore", "--no-overwrite-directories", snapID, restoreDir)

	// Attempt to restore snapshot with an already-existing target directory
	// It should fail because target files already exist
	_ = os.MkdirAll(restoreFailDir, 0o700)

	e.RunAndExpectFailure(t, "snapshot", "restore", "--no-overwrite-files", snapID, restoreDir)
}

func TestRestoreSymlinkWithoutTarget(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	source := t.TempDir()

	lnk := filepath.Join(source, "lnk")

	if err := os.Symlink(".no-such-file", lnk); err != nil {
		t.Fatal(err)
	}

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

	restoredDir := t.TempDir()
	e.RunAndExpectSuccess(t, "snapshot", "restore", "--no-ignore-permission-errors", snapID, restoredDir)
}

func TestRestoreSnapshotOfSingleFile(t *testing.T) {
	t.Parallel()

	e := testenv.NewCLITest(t)
	defer e.RunAndExpectSuccess(t, "repo", "disconnect")

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	sourceDir := t.TempDir()
	sourceFile := filepath.Join(sourceDir, "single-file")

	f, err := os.Create(sourceFile)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Fprintf(f, "some-data")
	f.Close()

	// set up unique file mode which will be verified later.
	os.Chmod(sourceFile, 0o653)

	e.RunAndExpectSuccess(t, "snapshot", "create", sourceFile)

	// obtain snapshot root id and use it for restore
	si := e.ListSnapshotsAndExpectSuccess(t, sourceFile)
	if got, want := len(si), 1; got != want {
		t.Fatalf("got %v sources, wanted %v", got, want)
	}

	if got, want := len(si[0].Snapshots), 1; got != want {
		t.Fatalf("got %v snapshots, wanted %v", got, want)
	}

	snapID := si[0].Snapshots[0].SnapshotID
	rootID := si[0].Snapshots[0].ObjectID

	restoreDir := t.TempDir()

	// restoring a file to a directory destination fails.
	e.RunAndExpectFailure(t, "snapshot", "restore", snapID, restoreDir)

	e.RunAndExpectSuccess(t, "snapshot", "restore", snapID, filepath.Join(restoreDir, "restored-1"))
	verifyFileMode(t, filepath.Join(restoreDir, "restored-1"), os.FileMode(0o653))

	// we can restore using rootID because it's unambiguous
	e.RunAndExpectSuccess(t, "snapshot", "restore", rootID, filepath.Join(restoreDir, "restored-2"))
	verifyFileMode(t, filepath.Join(restoreDir, "restored-2"), os.FileMode(0o653))

	if runtime.GOOS != windowsOSName {
		// change source file permissions and create one more snapshot
		// at this poing we will have multiple snapshot manifests with one root but different attributes.
		os.Chmod(sourceFile, 0o654)
		e.RunAndExpectSuccess(t, "snapshot", "create", sourceFile)

		// when restoring by root Kopia needs to pick which manifest to apply since they are conflicting
		// We're passing --consistent-attributes which causes it to fail, since otherwise we'd restore arbitrary
		// top-level object permissions.
		e.RunAndExpectFailure(t, "snapshot", "restore", rootID, "--consistent-attributes", filepath.Join(restoreDir, "restored-3"))
	}

	// Without the flag kopia picks the attributes from the latest snapshot.
	e.RunAndExpectSuccess(t, "snapshot", "restore", rootID, filepath.Join(restoreDir, "restored-3"))

	verifyFileMode(t, filepath.Join(restoreDir, "restored-3"), os.FileMode(0o654))

	// restoring using snapshot ID is unambiguous and always produces file with 0o653
	e.RunAndExpectSuccess(t, "snapshot", "restore", snapID, filepath.Join(restoreDir, "restored-4"))
	verifyFileMode(t, filepath.Join(restoreDir, "restored-4"), os.FileMode(0o653))

	// skip permissions when restoring, which results in default defaultRestoredFilePermission
	e.RunAndExpectSuccess(t, "snapshot", "restore", rootID, "--skip-permissions", filepath.Join(restoreDir, "restored-5"))

	verifyFileMode(t, filepath.Join(restoreDir, "restored-5"), defaultRestoredFilePermission)
}

func verifyFileMode(t *testing.T, filename string, want os.FileMode) {
	t.Helper()

	if runtime.GOOS == windowsOSName {
		// do not compare Unix filemodes on Windows.
		return
	}

	s, err := os.Lstat(filename)
	if err != nil {
		t.Fatal(err)
	}

	// make sure we restored permissions correctly
	if s.Mode() != want {
		t.Fatalf("invalid mode on %v: %v, want %v", filename, s.Mode(), want)
	}
}

func verifyValidZipFile(t *testing.T, fname string) {
	t.Helper()

	zr, err := zip.OpenReader(fname)
	if err != nil {
		t.Fatal(err)
	}

	defer zr.Close()
}

func verifyValidTarFile(t *testing.T, fname string) {
	t.Helper()

	f, err := os.Open(fname)
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}

	verifyValidTarReader(t, tar.NewReader(gz))
}
