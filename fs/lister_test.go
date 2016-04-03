package fs

import (
	"fmt"
	"io/ioutil"
	"log"
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

	ae := readAllEntries(dir)

	if len(ae) > 0 {
		t.Errorf("expected empty directory, got %v", ae)
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

	ae = readAllEntries(dir)

	goodCount := 0

	if ae[0].Name == "f1" && ae[0].Size == 5 && ae[0].Type == EntryTypeFile {
		goodCount++
	}
	if ae[1].Name == "f2" && ae[1].Size == 4 && ae[1].Type == EntryTypeFile {
		goodCount++
	}
	if ae[2].Name == "f3" && ae[2].Size == 3 && ae[2].Type == EntryTypeFile {
		goodCount++
	}
	if ae[3].Name == "y" && ae[3].Size == 0 && ae[3].Type == EntryTypeDirectory {
		goodCount++
	}
	if ae[4].Name == "z" && ae[4].Size == 0 && ae[4].Type == EntryTypeDirectory {
		goodCount++
	}
	if goodCount != 5 {
		t.Errorf("invalid dir data:\n%v", ae)
	}
}

func readAllEntries(dir Directory) []*Entry {
	var entries []*Entry
	for d := range dir {
		if d.Error != nil {
			log.Fatalf("got error listing directory: %v", d.Error)
		}
		entries = append(entries, d.Entry)
	}
	return entries
}
