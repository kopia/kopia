package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteFiles(t *testing.T) {
	r, err := NewRunner()
	require.NoError(t, err)

	defer r.Cleanup()

	relativeWritePath := "some/path/to/check"
	writeFileSizeB := int64(256 * 1024) // 256 KiB
	numFiles := 13
	fioOpt := Options{}.WithFileSize(writeFileSizeB).WithNumFiles(numFiles).WithBlockSize(4096)

	// Test a call to WriteFiles
	err = r.WriteFiles(relativeWritePath, fioOpt)
	require.NoError(t, err)

	fullPath := filepath.Join(r.LocalDataDir, relativeWritePath)
	dirEntries, err := os.ReadDir(fullPath)
	require.NoError(t, err)

	if got, want := len(dirEntries), numFiles; got != want {
		t.Errorf("Did not get expected number of files %v (actual) != %v (expected", got, want)
	}

	sizeTot := int64(0)

	for _, entry := range dirEntries {
		fi, err := entry.Info()
		if err != nil {
			t.Fatalf("Failed to read file info: %v", err)
		}

		sizeTot += fi.Size()
	}

	want := writeFileSizeB * int64(numFiles)
	if got := sizeTot; got != want {
		t.Errorf("Did not get the expected amount of data written %v (actual) != %v (expected)", got, want)
	}
}

func TestWriteFilesAtDepth(t *testing.T) {
	r, err := NewRunner()
	require.NoError(t, err)

	defer r.Cleanup()

	for _, tt := range []struct {
		name         string
		depth        int
		expFileCount int
	}{
		{
			name:         "Test depth zero, 1 file",
			depth:        0,
			expFileCount: 1,
		},
		{
			name:         "Test depth zero, multiple files",
			depth:        0,
			expFileCount: 10,
		},
		{
			name:         "Test depth 1, 1 file",
			depth:        1,
			expFileCount: 1,
		},
		{
			name:         "Test depth 1, multiple files",
			depth:        1,
			expFileCount: 10,
		},
		{
			name:         "Test depth 10, 1 file",
			depth:        10,
			expFileCount: 1,
		},
		{
			name:         "Test depth 10, multiple files",
			depth:        10,
			expFileCount: 10,
		},
	} {
		t.Log(tt.name)

		testWriteAtDepth(t, r, tt.depth, tt.expFileCount)
	}
}

func testWriteAtDepth(t *testing.T, r *Runner, depth, expFileCount int) {
	t.Helper()

	testSubdir := "test"
	testDirAbs := filepath.Join(r.LocalDataDir, testSubdir)

	sizeB := int64(128 * 1024 * 1024)
	fioOpt := Options{}.WithSize(sizeB).WithNumFiles(expFileCount)

	err := r.WriteFilesAtDepth(testSubdir, depth, fioOpt)
	require.NoError(t, err)

	defer r.DeleteRelDir(testSubdir)

	dirCount := 0
	fileCount := 0

	err = filepath.Walk(testDirAbs, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			dirCount++
		} else {
			fileCount++
		}

		return nil
	})
	require.NoError(t, err)

	if got, want := fileCount, expFileCount; got != want {
		t.Errorf("Expected %v files, only found %v", want, got)
	}

	// Expect number of directories to equal the requested depth, plus one
	// since the walk starts in a directory
	if got, want := dirCount, depth+1; got != want {
		t.Errorf("Expected %v directories, but found %v", want, got)
	}
}

func TestDeleteFilesAtDepth(t *testing.T) {
	r, err := NewRunner()
	require.NoError(t, err)

	defer r.Cleanup()

	for _, tt := range []struct {
		name        string
		wrDepth     int
		delDepth    int
		expDirCount int
		expErr      bool
	}{
		{
			name:        "Test write files at depth 1, delete a directory at depth 0 (expect error - can't delete root directory)",
			wrDepth:     1,
			delDepth:    0,
			expDirCount: 1,
			expErr:      true,
		},
		{
			name:        "Test write files at depth 1, delete a directory at depth 1",
			wrDepth:     1,
			delDepth:    1,
			expDirCount: 0,
			expErr:      false,
		},
		{
			name:        "Test write files at depth 10, delete a directory at depth 9",
			wrDepth:     10,
			delDepth:    9,
			expDirCount: 8,
			expErr:      false,
		},
		{
			name:        "Test write files at depth 10, delete a directory at depth 10",
			wrDepth:     10,
			delDepth:    10,
			expDirCount: 9,
			expErr:      false,
		},
		{
			name:        "Test write files at depth 1, delete a directory at depth 11 (expect error)",
			wrDepth:     1,
			delDepth:    11,
			expDirCount: 1,
			expErr:      true,
		},
	} {
		t.Log(tt.name)

		testDeleteAtDepth(t, r, tt.wrDepth, tt.delDepth, tt.expDirCount, tt.expErr)
	}
}

func testDeleteAtDepth(t *testing.T, r *Runner, wrDepth, delDepth, expDirCount int, expErr bool) {
	t.Helper()

	testSubdir := "test"
	testDirAbs := filepath.Join(r.LocalDataDir, testSubdir)

	sizeB := int64(128 * 1024 * 1024)
	numFiles := 2
	fioOpt := Options{}.WithSize(sizeB).WithNumFiles(numFiles)

	err := r.WriteFilesAtDepth(testSubdir, wrDepth, fioOpt)
	require.NoError(t, err)

	defer r.DeleteRelDir(testSubdir)

	err = r.DeleteDirAtDepth(testSubdir, delDepth)
	if expErr {
		if err == nil {
			t.Fatalf("Expected error but got none")
		}
	} else {
		require.NoError(t, err)
	}

	dirCount := 0
	fileCount := 0

	err = filepath.Walk(testDirAbs, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			dirCount++
		} else {
			fileCount++
		}

		return nil
	})
	require.NoError(t, err)

	// Expect number of directories to equal the requested depth, plus one
	// since the walk starts in a directory
	if got, want := dirCount, expDirCount+1; got != want {
		t.Errorf("Expected %v directories, but found %v", want, got)
	}
}

func TestDeleteContentsAtDepth(t *testing.T) {
	for _, tc := range []struct {
		prob                float32
		expFileCountChecker func(t *testing.T, fileCount int)
	}{
		{
			prob: 0.0,
			expFileCountChecker: func(t *testing.T, fileCount int) {
				t.Helper()

				if fileCount != 100 {
					t.Error("some files were deleted despite 0% probability")
				}
			},
		},
		{
			prob: 1.0,
			expFileCountChecker: func(t *testing.T, fileCount int) {
				t.Helper()

				if fileCount != 0 {
					t.Error("not all files were deleted despite 100% probability")
				}
			},
		},
		{
			prob: 0.5,
			expFileCountChecker: func(t *testing.T, fileCount int) {
				t.Helper()

				// Broad check: just make sure a 50% probability deleted something.
				// Extremely improbable that this causes a false failure;
				// akin to 100 coin flips all landing on the same side.
				if !(fileCount > 0 && fileCount < 100) {
					t.Error("expected some but not all files to be deleted")
				}
			},
		},
	} {
		testDeleteContentsAtDepth(t, tc.prob, tc.expFileCountChecker)
	}
}

//nolint:thelper
func testDeleteContentsAtDepth(t *testing.T, prob float32, checker func(t *testing.T, fileCount int)) {
	r, err := NewRunner()
	require.NoError(t, err)

	defer r.Cleanup()

	testSubdir := "test"
	testDirAbs := filepath.Join(r.LocalDataDir, testSubdir)

	sizeB := int64(128 * 1024 * 1024)
	numFiles := 100
	fioOpt := Options{}.WithSize(sizeB).WithNumFiles(numFiles)

	wrDepth := 3
	err = r.WriteFilesAtDepth(testSubdir, wrDepth, fioOpt)
	require.NoError(t, err)

	defer r.DeleteRelDir(testSubdir)

	delDepth := 3
	err = r.DeleteContentsAtDepth(testSubdir, delDepth, prob)
	require.NoError(t, err)

	fileCount := 0

	err = filepath.Walk(testDirAbs, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			fileCount++
		}

		return nil
	})

	require.NoError(t, err)

	checker(t, fileCount)
}
