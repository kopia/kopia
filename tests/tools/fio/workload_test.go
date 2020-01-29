package fio

import (
	"fmt"
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
	writeSizeB := int64(256 * 1024 * 1024) // 256 MiB
	numFiles := 13

	// Test a call to WriteFiles
	err = r.WriteFiles(relativeWritePath, writeSizeB, numFiles, Options{})
	testenv.AssertNoError(t, err)

	fullPath := filepath.Join(r.DataDir, relativeWritePath)
	dir, err := ioutil.ReadDir(fullPath)
	testenv.AssertNoError(t, err)

	if got, want := len(dir), numFiles; got != want {
		t.Errorf("Did not get expected number of files %v (actual) != %v (expected", got, want)
	}

	sizeTot := int64(0)

	for _, fi := range dir {
		fmt.Println(fi.Name(), fi.Size())
		sizeTot += fi.Size()
	}

	want := (writeSizeB / int64(numFiles)) * int64(numFiles)
	if got := sizeTot; got != want {
		t.Errorf("Did not get the expected amount of data written %v (actual) != %v (expected)", got, want)
	}
}
