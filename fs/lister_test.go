package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"testing"
)

func TestLister(t *testing.T) {
	var err error

	tmp, err := ioutil.TempDir("", "kopia")
	if err != nil {
		t.Errorf("cannot create temp directory: %v", err)
		return
	}

	defer os.RemoveAll(tmp)

	lister := &filesystemLister{}

	var dir Directory

	// Try listing directory that does not exist.
	dir, err = lister.List(fmt.Sprintf("/no-such-dir-%v", time.Now().Nanosecond()))
	if err == nil {
		t.Errorf("expected error when dir directory that does not exist.")
	}

	// Now list an empty directory that does not exist.
	dir, err = lister.List(tmp)
	if err != nil {
		t.Errorf("error when dir empty directory: %v", err)
	}

	if len(dir) > 0 {
		t.Errorf("expected empty directory, got %v", dir)
	}

	// Now list a directory with 3 files.
	ioutil.WriteFile(filepath.Join(tmp, "f3"), []byte{1, 2, 3}, 0777)
	ioutil.WriteFile(filepath.Join(tmp, "f2"), []byte{1, 2, 3, 4}, 0777)
	ioutil.WriteFile(filepath.Join(tmp, "f1"), []byte{1, 2, 3, 4, 5}, 0777)

	os.Mkdir(filepath.Join(tmp, "z"), 0777)
	os.Mkdir(filepath.Join(tmp, "y"), 0777)

	dir, err = lister.List(tmp)
	if err != nil {
		t.Errorf("error when dir directory with files: %v", err)
	}

	goodCount := 0

	if dir[0].Name() == "f1" && dir[0].Size() == 5 && dir[0].Mode().IsRegular() {
		goodCount++
	}
	if dir[1].Name() == "f2" && dir[1].Size() == 4 && dir[1].Mode().IsRegular() {
		goodCount++
	}
	if dir[2].Name() == "f3" && dir[2].Size() == 3 && dir[2].Mode().IsRegular() {
		goodCount++
	}
	if dir[3].Name() == "y" && dir[3].Size() == 0 && dir[3].Mode().IsDir() {
		goodCount++
	}
	if dir[4].Name() == "z" && dir[4].Size() == 0 && dir[4].Mode().IsDir() {
		goodCount++
	}
	if goodCount != 5 {
		t.Errorf("invalid dir data:\n%v", dir)
	}
}
