package fio

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestWriteFiles(t *testing.T) {
	r, err := NewRunner()
	testenv.AssertNoError(t, err)

	defer r.Cleanup()

	relativeWritePath := "some/path/to/check"
	writeFileSizeB := int64(256 * 1024) // 256 KiB
	numFiles := 13
	fioOpt := Options{}.WithFileSize(writeFileSizeB).WithNumFiles(numFiles).WithBlockSize(4096)

	// Test a call to WriteFiles
	err = r.WriteFiles(relativeWritePath, fioOpt)
	testenv.AssertNoError(t, err)

	fullPath := filepath.Join(r.LocalDataDir, relativeWritePath)
	dir, err := ioutil.ReadDir(fullPath)
	testenv.AssertNoError(t, err)

	if got, want := len(dir), numFiles; got != want {
		t.Errorf("Did not get expected number of files %v (actual) != %v (expected", got, want)
	}

	sizeTot := int64(0)

	for _, fi := range dir {
		sizeTot += fi.Size()
	}

	want := writeFileSizeB * int64(numFiles)
	if got := sizeTot; got != want {
		t.Errorf("Did not get the expected amount of data written %v (actual) != %v (expected)", got, want)
	}
}

func TestWriteFilesAtDepth(t *testing.T) {
	r, err := NewRunner()
	testenv.AssertNoError(t, err)

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
	testSubdir := "test"
	testDirAbs := filepath.Join(r.LocalDataDir, testSubdir)

	sizeB := int64(128 * 1024 * 1024)
	fioOpt := Options{}.WithSize(sizeB).WithNumFiles(expFileCount)

	err := r.WriteFilesAtDepth(testSubdir, depth, fioOpt)
	testenv.AssertNoError(t, err)

	defer r.DeleteRelDir(testSubdir)

	dirCount := 0
	fileCount := 0

	err = filepath.Walk(testDirAbs, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		fmt.Println(path, info.Name())
		if info.IsDir() {
			dirCount++
		} else {
			fileCount++
		}

		return nil
	})
	testenv.AssertNoError(t, err)

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
	testenv.AssertNoError(t, err)

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
	testSubdir := "test"
	testDirAbs := filepath.Join(r.LocalDataDir, testSubdir)

	sizeB := int64(128 * 1024 * 1024)
	numFiles := 2
	fioOpt := Options{}.WithSize(sizeB).WithNumFiles(numFiles)

	err := r.WriteFilesAtDepth(testSubdir, wrDepth, fioOpt)
	testenv.AssertNoError(t, err)

	defer r.DeleteRelDir(testSubdir)

	err = r.DeleteDirAtDepth(testSubdir, delDepth)
	if expErr {
		if err == nil {
			t.Fatalf("Expected error but got none")
		}
	} else {
		testenv.AssertNoError(t, err)
	}

	dirCount := 0
	fileCount := 0

	err = filepath.Walk(testDirAbs, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		fmt.Println(path)
		if info.IsDir() {
			dirCount++
		} else {
			fileCount++
		}

		return nil
	})
	testenv.AssertNoError(t, err)

	// Expect number of directories to equal the requested depth, plus one
	// since the walk starts in a directory
	if got, want := dirCount, expDirCount+1; got != want {
		t.Errorf("Expected %v directories, but found %v", want, got)
	}
}
