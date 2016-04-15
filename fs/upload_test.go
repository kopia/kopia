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

	oid, metadataOID, err := u.UploadDir(sourceDir, "")
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	oid2, metadataOID2, err := u.UploadDir(sourceDir, oid)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if oid2 != oid {
		t.Errorf("expected oid==oid2, got %v and %v", oid, oid2)
	}

	if metadataOID2 != metadataOID {
		t.Errorf("expected metadataOID2==metadataOID, got %v and %v", metadataOID2, metadataOID)
	}

	// Add one more file, the oid should change.
	ioutil.WriteFile(filepath.Join(sourceDir, "d2/d1/f3"), []byte{1, 2, 3, 4, 5}, 0777)
	oid3, metadataOID3, err := u.UploadDir(sourceDir, oid)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if oid2 == oid3 {
		t.Errorf("expected oid3!=oid2, got %v", oid3)
	}

	if metadataOID2 == metadataOID3 {
		t.Errorf("expected metadataOID3!=metadataOID2, got %v", metadataOID3)
	}

	// Now remove the added file, OID should be identical to the original before the file got added.
	os.Remove(filepath.Join(sourceDir, "d2/d1/f3"))

	oid4, metadataOID4, err := u.UploadDir(sourceDir, "")
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if oid4 != oid {
		t.Errorf("expected oid3==oid, got %v and %v", oid4, oid)
	}
	if metadataOID4 != metadataOID {
		t.Errorf("expected metadataOID3==metadataOID4, got %v and %v", metadataOID4, metadataOID)
	}
}
