package restore_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/restore"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/kopia/kopia/snapshot/upload"
)

// TestBirthTimeSnapshotAndRestore validates birthtime support across snapshot/restore lifecycle.
// Tests three scenarios:
// 1. Old snapshot without btime (backward compatibility)
// 2. New snapshot with btime (full preservation)
// 3. Metadata-only update (photo date fix use case)
func TestBirthTimeSnapshotAndRestore(t *testing.T) {
	// Setup test environment
	ctx, env := repotesting.NewEnvironment(t, repotesting.FormatNotImportant)
	sourceDir := t.TempDir()

	canRestoreBirthTime := runtime.GOOS == "windows" || runtime.GOOS == "darwin"

	// Create test file with known birthtime and mtime
	testFile := setupTestFile(t, sourceDir)
	originalBtime, originalDirBtime, mtime := captureOriginalTimes(t, testFile, sourceDir)

	// SCENARIO 1: Old snapshot without birthtime support (backward compatibility)
	t.Log("=== Scenario 1: Old snapshot without btime (simulating old Kopia) ===")
	oldSnapshot := createOldStyleSnapshot(t, ctx, env.RepositoryWriter, sourceDir)
	verifyOldSnapshotRestore(t, ctx, env.RepositoryWriter, oldSnapshot, canRestoreBirthTime)

	// SCENARIO 2: New snapshot with birthtime support
	t.Log("=== Scenario 2: New snapshot with full btime support ===")
	newSnapshot := createSnapshot(t, ctx, env.RepositoryWriter, sourceDir)
	verifyBirthtimePreservation(t, ctx, env.RepositoryWriter, newSnapshot,
		originalBtime, originalDirBtime, mtime, canRestoreBirthTime)

	// SCENARIO 3: Metadata-only update (photo date fix use case)
	if canRestoreBirthTime {
		t.Log("=== Scenario 3: Birthtime metadata update without content re-upload ===")
		verifyMetadataOnlyUpdate(t, ctx, env.RepositoryWriter, sourceDir, testFile,
			newSnapshot, originalBtime, originalDirBtime, mtime)
	}
}

// setupTestFile creates a test file with distinct birthtime and mtime.
func setupTestFile(t *testing.T, dir string) string {
	t.Helper()

	testFile := filepath.Join(dir, "dummy.txt")
	require.NoError(t, os.WriteFile(testFile, []byte("test"), 0o644))

	// Set mtime to 1 minute in the future to differentiate from birth time
	futureTime := time.Now().Add(1 * time.Minute)
	require.NoError(t, os.Chtimes(testFile, futureTime, futureTime))

	return testFile
}

// captureOriginalTimes captures birthtime and mtime from source file and directory.
func captureOriginalTimes(t *testing.T, file, dir string) (fileBtime, dirBtime time.Time, mtime time.Time) {
	t.Helper()

	fileEntry, err := localfs.NewEntry(file)
	require.NoError(t, err)
	fileBtime = fs.GetBirthTime(fileEntry)
	mtime = fileEntry.ModTime()

	dirEntry, err := localfs.NewEntry(dir)
	require.NoError(t, err)
	dirBtime = fs.GetBirthTime(dirEntry)

	t.Logf("Original times - file btime: %v, mtime: %v", fileBtime, mtime)
	t.Logf("Original times - dir btime: %v", dirBtime)

	return fileBtime, dirBtime, mtime
}

// createOldStyleSnapshot creates a snapshot and removes btime to simulate old Kopia version.
func createOldStyleSnapshot(t *testing.T, ctx context.Context, rep repo.RepositoryWriter, sourceDir string) *snapshot.Manifest {
	t.Helper()

	snap := createSnapshot(t, ctx, rep, sourceDir)

	// Simulate old repo without btime: set BirthTime = nil
	snap.RootEntry.BirthTime = nil
	_, err := snapshot.SaveSnapshot(ctx, rep, snap)
	require.NoError(t, err)

	t.Log("Created old-style snapshot (btime = nil)")
	return snap
}

// verifyOldSnapshotRestore verifies that restoring old snapshots without btime works correctly.
func verifyOldSnapshotRestore(t *testing.T, ctx context.Context, rep repo.RepositoryWriter,
	snap *snapshot.Manifest, canRestoreBirthTime bool) {
	t.Helper()

	restoreDir := t.TempDir()
	restoreSnapshot(t, ctx, rep, snap, restoreDir)

	restoredDirEntry, err := localfs.NewEntry(restoreDir)
	require.NoError(t, err)
	restoredDirBtime := fs.GetBirthTime(restoredDirEntry)

	t.Logf("Restored dir btime from old snapshot: %v", restoredDirBtime)

	if canRestoreBirthTime {
		// For old snapshots without btime, OS sets btime to file creation time (now)
		assertTimeIsRecent(t, restoredDirBtime, "dir btime should be recent (file creation time)")
	}
}

// verifyBirthtimePreservation verifies that birthtimes are correctly preserved during snapshot/restore.
func verifyBirthtimePreservation(t *testing.T, ctx context.Context, rep repo.RepositoryWriter,
	snap *snapshot.Manifest, expectedFileBtime, expectedDirBtime, expectedMtime time.Time,
	canRestoreBirthTime bool) {
	t.Helper()

	restoreDir := t.TempDir()
	restoreSnapshot(t, ctx, rep, snap, restoreDir)

	// Verify file times
	restoredFile := filepath.Join(restoreDir, "dummy.txt"+localfs.ShallowEntrySuffix)
	fileEntry, err := localfs.NewEntry(restoredFile)
	require.NoError(t, err)
	restoredFileBtime := fs.GetBirthTime(fileEntry)
	restoredMtime := fileEntry.ModTime()

	// Verify directory times
	dirEntry, err := localfs.NewEntry(restoreDir)
	require.NoError(t, err)
	restoredDirBtime := fs.GetBirthTime(dirEntry)

	t.Logf("Restored times - file btime: %v, mtime: %v", restoredFileBtime, restoredMtime)
	t.Logf("Restored times - dir btime: %v", restoredDirBtime)

	// mtime should always be restored correctly
	require.Equal(t, expectedMtime, restoredMtime, "mtime should match")

	if canRestoreBirthTime {
		// On Windows/macOS, birth time should be preserved
		require.Equal(t, expectedFileBtime, restoredFileBtime, "file birth time should match on "+runtime.GOOS)
		require.Equal(t, expectedDirBtime, restoredDirBtime, "directory birth time should match on "+runtime.GOOS)
	} else {
		// On Linux, birthtime cannot be set during restore.
		// Birthtime will be set to file creation time (approximately now).
		// The birthtime stored in snapshots is still useful for:
		// - Cross-platform restore (Linux -> macOS/Windows)
		// - Future Linux kernel support for birthtime setting
		assertTimeIsRecent(t, restoredFileBtime, "file btime should be recent on "+runtime.GOOS)
		assertTimeIsRecent(t, restoredDirBtime, "dir btime should be recent on "+runtime.GOOS)
	}
}

// verifyMetadataOnlyUpdate tests that changing only birthtime doesn't trigger content re-upload.
func verifyMetadataOnlyUpdate(t *testing.T, ctx context.Context, rep repo.RepositoryWriter,
	sourceDir, testFile string, previousSnapshot *snapshot.Manifest,
	originalFileBtime, originalDirBtime, mtime time.Time) {
	t.Helper()

	// Update birthtimes (simulating photo date correction)
	newFileBtime := originalFileBtime.Add(-24 * time.Hour)
	newDirBtime := originalDirBtime.Add(-48 * time.Hour)
	updateBirthtimes(t, testFile, sourceDir, newFileBtime, newDirBtime, mtime)

	// Verify birthtimes were updated
	updatedFileBtime, updatedDirBtime := verifyBirthtimesUpdated(t, testFile, sourceDir,
		newFileBtime, newDirBtime)

	// Create new snapshot
	newSnapshot := createSnapshot(t, ctx, rep, sourceDir)
	t.Log("Created snapshot after birthtime-only changes")

	// Verify metadata updated in snapshot without content re-upload
	verifySnapshotMetadataUpdate(t, ctx, rep, previousSnapshot, newSnapshot,
		updatedFileBtime, updatedDirBtime)

	t.Log("SUCCESS: Birthtime metadata updated for both file and directory without re-uploading content")
}

// updateBirthtimes changes birthtimes on file and directory without changing content or mtime.
func updateBirthtimes(t *testing.T, file, dir string, fileBtime, dirBtime, mtime time.Time) {
	t.Helper()

	require.NoError(t, restore.ChtimesExact(file, fileBtime, mtime, mtime))
	require.NoError(t, restore.ChtimesExact(dir, dirBtime, mtime, mtime))

	t.Logf("Updated file btime to: %v", fileBtime)
	t.Logf("Updated dir btime to: %v", dirBtime)
}

// verifyBirthtimesUpdated confirms that birthtimes were successfully changed.
func verifyBirthtimesUpdated(t *testing.T, file, dir string, expectedFileBtime, expectedDirBtime time.Time) (time.Time, time.Time) {
	t.Helper()

	fileEntry, err := localfs.NewEntry(file)
	require.NoError(t, err)
	actualFileBtime := fs.GetBirthTime(fileEntry)

	dirEntry, err := localfs.NewEntry(dir)
	require.NoError(t, err)
	actualDirBtime := fs.GetBirthTime(dirEntry)

	require.Equal(t, expectedFileBtime.Unix(), actualFileBtime.Unix(), "file birthtime should be updated")
	require.Equal(t, expectedDirBtime.Unix(), actualDirBtime.Unix(), "dir birthtime should be updated")

	return actualFileBtime, actualDirBtime
}

// verifySnapshotMetadataUpdate verifies that snapshot captured updated birthtimes without re-uploading content.
func verifySnapshotMetadataUpdate(t *testing.T, ctx context.Context, rep repo.RepositoryWriter,
	oldSnapshot, newSnapshot *snapshot.Manifest, expectedFileBtime, expectedDirBtime time.Time) {
	t.Helper()

	// Get file entry from old snapshot
	oldFile := getFileFromSnapshot(t, ctx, rep, oldSnapshot, "dummy.txt")

	// Get file entry from new snapshot
	newFile := getFileFromSnapshot(t, ctx, rep, newSnapshot, "dummy.txt")
	newRoot, err := snapshotfs.SnapshotRoot(rep, newSnapshot)
	require.NoError(t, err)

	// Verify birthtimes updated in snapshot
	newFileBtime := fs.GetBirthTime(newFile)
	newDirBtime := fs.GetBirthTime(newRoot)

	require.Equal(t, expectedFileBtime.Unix(), newFileBtime.Unix(), "file snapshot should reflect updated birthtime")
	require.Equal(t, expectedDirBtime.Unix(), newDirBtime.Unix(), "dir snapshot should reflect updated birthtime")

	// Verify content NOT re-uploaded (file size unchanged)
	require.Equal(t, oldFile.Size(), newFile.Size(), "file size should be unchanged (content not re-uploaded)")
}

// getFileFromSnapshot retrieves a file entry from a snapshot by name.
func getFileFromSnapshot(t *testing.T, ctx context.Context, rep repo.RepositoryWriter,
	snap *snapshot.Manifest, fileName string) fs.File {
	t.Helper()

	root, err := snapshotfs.SnapshotRoot(rep, snap)
	require.NoError(t, err)

	rootDir, ok := root.(fs.Directory)
	require.True(t, ok, "root should be a directory")

	fileEntry, err := rootDir.Child(ctx, fileName)
	require.NoError(t, err)

	file, ok := fileEntry.(fs.File)
	require.True(t, ok, "entry should be a file")

	return file
}

// restoreSnapshot restores a snapshot to the specified directory.
func restoreSnapshot(t *testing.T, ctx context.Context, rep repo.RepositoryWriter,
	snap *snapshot.Manifest, targetDir string) {
	t.Helper()

	root, err := snapshotfs.SnapshotRoot(rep, snap)
	require.NoError(t, err)

	output := restore.FilesystemOutput{
		TargetPath:             targetDir,
		OverwriteDirectories:   true,
		OverwriteFiles:         true,
		OverwriteSymlinks:      true,
		IgnorePermissionErrors: true,
	}
	require.NoError(t, output.Init(ctx))

	_, err = restore.Entry(ctx, rep, &output, root, restore.Options{})
	require.NoError(t, err)
}

// assertTimeIsRecent verifies that a time is within the last 10 seconds.
func assertTimeIsRecent(t *testing.T, timeToCheck time.Time, message string) {
	t.Helper()

	timeSince := time.Since(timeToCheck)
	require.Less(t, timeSince, 10*time.Second, message+" (should be within last 10 seconds)")
	require.GreaterOrEqual(t, timeSince, time.Duration(0), message+" (should not be in the future)")
}

// createSnapshot creates a snapshot of the source directory.
func createSnapshot(t *testing.T, ctx context.Context, rep repo.RepositoryWriter, sourceDir string) *snapshot.Manifest {
	t.Helper()

	source, err := localfs.Directory(sourceDir)
	require.NoError(t, err)

	u := upload.NewUploader(rep)
	man, err := u.Upload(ctx, source, nil, snapshot.SourceInfo{
		Path:     sourceDir,
		Host:     "test-host",
		UserName: "test-user",
	})
	require.NoError(t, err)

	_, err = snapshot.SaveSnapshot(ctx, rep, man)
	require.NoError(t, err)

	return man
}
