package localfs

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/kopia/kopia/fs"

	"testing"
)

//nolint:gocyclo
func TestFiles(t *testing.T) {
	ctx := context.Background()
	var err error

	tmp, err := ioutil.TempDir("", "kopia")
	if err != nil {
		t.Errorf("cannot create temp directory: %v", err)
		return
	}

	defer os.RemoveAll(tmp)

	var dir fs.Directory

	// Try listing directory that does not exist.
	_, err = Directory(fmt.Sprintf("/no-such-dir-%v", time.Now().Nanosecond()))
	if err == nil {
		t.Errorf("expected error when dir directory that does not exist.")
	}

	// Now list an empty directory that does exist.
	dir, err = Directory(tmp)
	if err != nil {
		t.Errorf("error when dir empty directory: %v", err)
	}

	entries, err := dir.Readdir(ctx)
	if err != nil {
		t.Errorf("error gettind dir Entries: %v", err)
	}

	if len(entries) > 0 {
		t.Errorf("expected empty directory, got %v", dir)
	}

	// Now list a directory with 3 files.
	assertNoError(t, ioutil.WriteFile(filepath.Join(tmp, "f3"), []byte{1, 2, 3}, 0777))
	assertNoError(t, ioutil.WriteFile(filepath.Join(tmp, "f2"), []byte{1, 2, 3, 4}, 0777))
	assertNoError(t, ioutil.WriteFile(filepath.Join(tmp, "f1"), []byte{1, 2, 3, 4, 5}, 0777))

	assertNoError(t, os.Mkdir(filepath.Join(tmp, "z"), 0777))
	assertNoError(t, os.Mkdir(filepath.Join(tmp, "y"), 0777))

	dir, err = Directory(tmp)
	if err != nil {
		t.Errorf("error when dir directory with files: %v", err)
	}

	entries, err = dir.Readdir(ctx)
	if err != nil {
		t.Errorf("error gettind dir Entries: %v", err)
	}

	goodCount := 0

	if entries[0].Name() == "f1" && entries[0].Size() == 5 && entries[0].Mode().IsRegular() {
		goodCount++
	}
	if entries[1].Name() == "f2" && entries[1].Size() == 4 && entries[1].Mode().IsRegular() {
		goodCount++
	}
	if entries[2].Name() == "f3" && entries[2].Size() == 3 && entries[2].Mode().IsRegular() {
		goodCount++
	}
	if entries[3].Name() == "y" && entries[3].Size() == 0 && entries[3].Mode().IsDir() {
		goodCount++
	}
	if entries[4].Name() == "z" && entries[4].Size() == 0 && entries[4].Mode().IsDir() {
		goodCount++
	}
	if goodCount != 5 {
		t.Errorf("invalid dir data: %v good entries", goodCount)
		for i, e := range entries {
			t.Logf("e[%v] = %v %v %v", i, e.Name(), e.Size(), e.Mode())
		}
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("err: %v", err)
	}
}
