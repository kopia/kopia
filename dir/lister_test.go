package dir

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

	var listing Listing

	// Try listing directory that does not exist.
	listing, err = lister.List(fmt.Sprintf("/no-such-dir-%v", time.Now().Nanosecond()))
	if err == nil {
		t.Errorf("expected error when listing directory that does not exist.")
	}

	// Now list an empty directory that does not exist.
	listing, err = lister.List(tmp)
	if err != nil {
		t.Errorf("error when listing empty directory: %v", err)
	}

	if len(listing.Entries) > 0 {
		t.Errorf("expected empty directory, got %v", listing)
	}

	// Now list a directory with 3 files.
	ioutil.WriteFile(filepath.Join(tmp, "f3"), []byte{1, 2, 3}, 0777)
	ioutil.WriteFile(filepath.Join(tmp, "f2"), []byte{1, 2, 3, 4}, 0777)
	ioutil.WriteFile(filepath.Join(tmp, "f1"), []byte{1, 2, 3, 4, 5}, 0777)

	os.Mkdir(filepath.Join(tmp, "z"), 0777)
	os.Mkdir(filepath.Join(tmp, "y"), 0777)

	listing, err = lister.List(tmp)
	if err != nil {
		t.Errorf("error when listing directory with files: %v", err)
	}

	if len(listing.Entries) != 5 {
		t.Errorf("expected 5 files, got: %v", listing)
	} else {
		goodCount := 0
		if listing.Entries[0].Name == "f1" && listing.Entries[0].Size == 5 && listing.Entries[0].Type == EntryTypeFile {
			goodCount++
		}
		if listing.Entries[1].Name == "f2" && listing.Entries[1].Size == 4 && listing.Entries[1].Type == EntryTypeFile {
			goodCount++
		}
		if listing.Entries[2].Name == "f3" && listing.Entries[2].Size == 3 && listing.Entries[2].Type == EntryTypeFile {
			goodCount++
		}
		if listing.Entries[3].Name == "y" && listing.Entries[3].Size == 0 && listing.Entries[3].Type == EntryTypeDirectory {
			goodCount++
		}
		if listing.Entries[4].Name == "z" && listing.Entries[4].Size == 0 && listing.Entries[4].Type == EntryTypeDirectory {
			goodCount++
		}
		if goodCount != 5 {
			t.Errorf("invalid listing data:\n%v", listing)
		}
	}
}
