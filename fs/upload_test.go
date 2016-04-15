package fs

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/cas"

	"testing"
)

func TestUpload(t *testing.T) {
	var err error

	sourceDir, err := ioutil.TempDir("", "kopia-src")
	if err != nil {
		t.Errorf("cannot create temp directory: %v", err)
		return
	}

	// Prepare directory contents.
	os.MkdirAll(filepath.Join(sourceDir, "d1/d1"), 0777)
	os.MkdirAll(filepath.Join(sourceDir, "d1/d2"), 0777)
	os.MkdirAll(filepath.Join(sourceDir, "d2/d1"), 0777)

	ioutil.WriteFile(filepath.Join(sourceDir, "f1"), []byte{1, 2, 3}, 0777)
	ioutil.WriteFile(filepath.Join(sourceDir, "f2"), []byte{1, 2, 3, 4}, 0777)
	ioutil.WriteFile(filepath.Join(sourceDir, "f3"), []byte{1, 2, 3, 4, 5}, 0777)

	ioutil.WriteFile(filepath.Join(sourceDir, "d1/d1/f1"), []byte{1, 2, 3}, 0777)
	ioutil.WriteFile(filepath.Join(sourceDir, "d1/d1/f2"), []byte{1, 2, 3, 4}, 0777)
	ioutil.WriteFile(filepath.Join(sourceDir, "d1/f2"), []byte{1, 2, 3, 4}, 0777)
	ioutil.WriteFile(filepath.Join(sourceDir, "d1/d2/f1"), []byte{1, 2, 3}, 0777)
	ioutil.WriteFile(filepath.Join(sourceDir, "d1/d2/f2"), []byte{1, 2, 3, 4}, 0777)
	ioutil.WriteFile(filepath.Join(sourceDir, "d2/d1/f1"), []byte{1, 2, 3}, 0777)
	ioutil.WriteFile(filepath.Join(sourceDir, "d2/d1/f2"), []byte{1, 2, 3, 4}, 0777)

	defer os.RemoveAll(sourceDir)

	repoDir, err := ioutil.TempDir("", "kopia-repo")
	if err != nil {
		t.Errorf("cannot create temp directory: %v", err)
		return
	}

	defer os.RemoveAll(repoDir)

	format := cas.Format{
		Version: "1",
		Hash:    "md5",
	}

	repo, err := blob.NewFSStorage(&blob.FSStorageOptions{
		Path: repoDir,
	})

	if err != nil {
		t.Errorf("unable to create repo: %v", err)
		return
	}

	objectManager, err := cas.NewObjectManager(repo, format)
	if err != nil {
		t.Errorf("unable to create object manager: %v", err)
		return
	}

	u, err := NewUploader(objectManager)
	if err != nil {
		t.Errorf("unable to create uploader: %v", err)
		return
	}

	r1, err := u.UploadDir(sourceDir, "")
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	r2, err := u.UploadDir(sourceDir, r1.ObjectID)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if r2.ObjectID != r1.ObjectID {
		t.Errorf("expected r1.ObjectID==r2.ObjectID, got %v and %v", r1.ObjectID, r2.ObjectID)
	}

	if r2.ManifestID != r1.ManifestID {
		t.Errorf("expected r2.ManifestID==r1.ManifestID, got %v and %v", r2.ManifestID, r1.ManifestID)
	}

	// Add one more file, the r1.ObjectID should change.
	ioutil.WriteFile(filepath.Join(sourceDir, "d2/d1/f3"), []byte{1, 2, 3, 4, 5}, 0777)
	r3, err := u.UploadDir(sourceDir, r1.ObjectID)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if r2.ObjectID == r3.ObjectID {
		t.Errorf("expected r3.ObjectID!=r2.ObjectID, got %v", r3.ObjectID)
	}

	if r2.ManifestID == r3.ManifestID {
		t.Errorf("expected r3.ManifestID!=r2.ManifestID, got %v", r3.ManifestID)
	}

	// Now remove the added file, OID should be identical to the original before the file got added.
	os.Remove(filepath.Join(sourceDir, "d2/d1/f3"))

	r4, err := u.UploadDir(sourceDir, "")
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if r4.ObjectID != r1.ObjectID {
		t.Errorf("expected r3.ObjectID==r1.ObjectID, got %v and %v", r4.ObjectID, r1.ObjectID)
	}
	if r4.ManifestID != r1.ManifestID {
		t.Errorf("expected r3.ManifestID==r4.ManifestID, got %v and %v", r4.ManifestID, r1.ManifestID)
	}
}
