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
	dir, err = Directory(fmt.Sprintf("/no-such-dir-%v", time.Now().Nanosecond()), nil)
	if err == nil {
		t.Errorf("expected error when dir directory that does not exist.")
	}

	// Now list an empty directory that does exist.
	dir, err = Directory(tmp, nil)
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
	ioutil.WriteFile(filepath.Join(tmp, "f3"), []byte{1, 2, 3}, 0777)
	ioutil.WriteFile(filepath.Join(tmp, "f2"), []byte{1, 2, 3, 4}, 0777)
	ioutil.WriteFile(filepath.Join(tmp, "f1"), []byte{1, 2, 3, 4, 5}, 0777)

	os.Mkdir(filepath.Join(tmp, "z"), 0777)
	os.Mkdir(filepath.Join(tmp, "y"), 0777)

	dir, err = Directory(tmp, nil)
	if err != nil {
		t.Errorf("error when dir directory with files: %v", err)
	}

	entries, err = dir.Readdir(ctx)
	if err != nil {
		t.Errorf("error gettind dir Entries: %v", err)
	}

	goodCount := 0

	if entries[0].Metadata().Name == "f1" && entries[0].Metadata().FileSize == 5 && entries[0].Metadata().FileMode().IsRegular() {
		goodCount++
	}
	if entries[1].Metadata().Name == "f2" && entries[1].Metadata().FileSize == 4 && entries[1].Metadata().FileMode().IsRegular() {
		goodCount++
	}
	if entries[2].Metadata().Name == "f3" && entries[2].Metadata().FileSize == 3 && entries[2].Metadata().FileMode().IsRegular() {
		goodCount++
	}
	if entries[3].Metadata().Name == "y" && entries[3].Metadata().FileSize == 0 && entries[3].Metadata().FileMode().IsDir() {
		goodCount++
	}
	if entries[4].Metadata().Name == "z" && entries[4].Metadata().FileSize == 0 && entries[4].Metadata().FileMode().IsDir() {
		goodCount++
	}
	if goodCount != 5 {
		t.Errorf("invalid dir data:\n%v", dir)
	}
}
