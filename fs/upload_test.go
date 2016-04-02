package fs

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/kopia/kopia/cas"
	"github.com/kopia/kopia/storage"

	"testing"
)

func TestUpload(t *testing.T) {
	var err error

	sourceDir, err := ioutil.TempDir("", "kopia-src")
	if err != nil {
		t.Errorf("cannot create temp directory: %v", err)
		return
	}

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

	repo, err := storage.NewFSRepository(&storage.FSRepositoryOptions{
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

	oid, err := u.UploadDir(sourceDir, "")
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}
	log.Printf("v = %#v", objectManager.Stats())

	log.Printf("oid: %v", oid)

	oid2, err := u.UploadDir(sourceDir, oid)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}
	log.Printf("v = %#v", objectManager.Stats())
	log.Printf("oid2: %v", oid2)

	if oid2 != oid {
		t.Errorf("expected oid==oid2, got %v and %v", oid, oid2)
	}

	ioutil.WriteFile(filepath.Join(sourceDir, "d2/d1/f3"), []byte{1, 2, 3, 4, 5}, 0777)
	oid3, err := u.UploadDir(sourceDir, oid2)
	log.Printf("v = %#v", objectManager.Stats())
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}
	log.Printf("oid3: %v", oid3)

	if oid2 == oid3 {
		t.Errorf("expected oid3!=oid2, got %v and %v", oid3, oid2)
	}

	oid4, err := u.UploadDir(sourceDir, "")
	log.Printf("v = %#v", objectManager.Stats())
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}
	log.Printf("oid4: %v", oid4)

	if oid3 != oid4 {
		t.Errorf("expected oid3!=oid4, got %v and %v", oid3, oid4)
	}
}
