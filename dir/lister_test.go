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

	lister := NewFilesystemLister()

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

	if len(listing.Directories)+len(listing.Files) > 0 {
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

	if len(listing.Files) != 3 || len(listing.Directories) != 2 {
		t.Errorf("expected 3 files, got: %v", listing)
	} else {
		goodCount := 0
		if listing.Files[0].Name == "f1" && listing.Files[0].Size == 5 && listing.Files[0].Type == EntryTypeFile {
			goodCount++
		}
		if listing.Files[1].Name == "f2" && listing.Files[1].Size == 4 && listing.Files[1].Type == EntryTypeFile {
			goodCount++
		}
		if listing.Files[2].Name == "f3" && listing.Files[2].Size == 3 && listing.Files[2].Type == EntryTypeFile {
			goodCount++
		}
		if listing.Directories[0].Name == "y" && listing.Directories[0].Size == 0 && listing.Directories[0].Type == EntryTypeDirectory {
			goodCount++
		}
		if listing.Directories[1].Name == "z" && listing.Directories[1].Size == 0 && listing.Directories[1].Type == EntryTypeDirectory {
			goodCount++
		}
		if goodCount != 5 {
			t.Errorf("invalid listing data:\n%v", listing)
		}
	}
}
