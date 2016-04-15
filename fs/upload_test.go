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

	r2, err := u.UploadDir(sourceDir, r1.ManifestID)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if r2.ObjectID != r1.ObjectID {
		t.Errorf("expected r1.ObjectID==r2.ObjectID, got %v and %v", r1.ObjectID, r2.ObjectID)
	}

	if r2.ManifestID != r1.ManifestID {
		t.Errorf("expected r2.ManifestID==r1.ManifestID, got %v and %v", r2.ManifestID, r1.ManifestID)
	}

	if r1.Stats.CachedFiles+r1.Stats.CachedDirectories != 0 {
		t.Errorf("unexpected r1 stats: %#v", r1.Stats)
	}

	// All non-cached files from r1 are now cached and there are no non-cached files or dirs since nothing changed.
	if r2.Stats.CachedFiles+r2.Stats.CachedDirectories != r1.Stats.NonCachedFiles+r1.Stats.NonCachedDirectories ||
		r2.Stats.NonCachedFiles+r2.Stats.NonCachedDirectories != 0 {
		t.Errorf("unexpected r2 stats: %#v", r2.Stats)
	}

	// Add one more file, the r1.ObjectID should change.
	ioutil.WriteFile(filepath.Join(sourceDir, "d2/d1/f3"), []byte{1, 2, 3, 4, 5}, 0777)
	r3, err := u.UploadDir(sourceDir, r1.ManifestID)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if r2.ObjectID == r3.ObjectID {
		t.Errorf("expected r3.ObjectID!=r2.ObjectID, got %v", r3.ObjectID)
	}

	if r2.ManifestID == r3.ManifestID {
		t.Errorf("expected r3.ManifestID!=r2.ManifestID, got %v", r3.ManifestID)
	}

	if r3.Stats.NonCachedFiles != 1 && r3.Stats.NonCachedDirectories != 3 {
		// one file is not cached, which causes "./d2/d1/", "./d2/" and "./" to be changed.
		t.Errorf("unexpected r3 stats: %#v", r3.Stats)
	}

	// Now remove the added file, OID should be identical to the original before the file got added.
	os.Remove(filepath.Join(sourceDir, "d2/d1/f3"))

	r4, err := u.UploadDir(sourceDir, r1.ManifestID)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if r4.ObjectID != r1.ObjectID {
		t.Errorf("expected r4.ObjectID==r1.ObjectID, got %v and %v", r4.ObjectID, r1.ObjectID)
	}
	if r4.ManifestID != r1.ManifestID {
		t.Errorf("expected r4.ManifestID==r1.ManifestID, got %v and %v", r4.ManifestID, r1.ManifestID)
	}

	// Everything is still cached.
	if r4.Stats.CachedFiles+r4.Stats.CachedDirectories != r1.Stats.NonCachedFiles+r1.Stats.NonCachedDirectories ||
		r4.Stats.NonCachedFiles+r4.Stats.NonCachedDirectories != 0 {
		t.Errorf("unexpected r4 stats: %#v", r4.Stats)
	}

	// Upload again, this time using r3.ManifestID as base.
	r5, err := u.UploadDir(sourceDir, r3.ManifestID)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if r5.ObjectID != r1.ObjectID {
		t.Errorf("expected r5.ObjectID==r1.ObjectID, got %v and %v", r5.ObjectID, r1.ObjectID)
	}
	if r5.ManifestID != r1.ManifestID {
		t.Errorf("expected r5.ManifestID==r1.ManifestID, got %v and %v", r5.ManifestID, r1.ManifestID)
	}

	if r3.Stats.NonCachedFiles != 0 && r3.Stats.NonCachedDirectories != 3 {
		// no files are changed, but one file disappeared which caused "./d2/d1/", "./d2/" and "./" to be changed.
		t.Errorf("unexpected r5 stats: %#v", r5.Stats)
	}
}
