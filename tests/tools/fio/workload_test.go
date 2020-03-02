package fio

import (
	"io/ioutil"
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
