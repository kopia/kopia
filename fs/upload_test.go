package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/kopia/kopia/blob"
	"github.com/kopia/kopia/cas"

	"testing"
)

type uploadTestHarness struct {
	sourceDir string
	repoDir   string
	repo      cas.Repository
	storage   blob.Storage
	lister    *perPathLister
	uploader  Uploader
}

var errTest = fmt.Errorf("test error")

type perPathLister struct {
	listFunc      map[string]func(path string) (Directory, error)
	openFunc      map[string]func(path string) (EntryReadCloser, error)
	defaultLister Lister
}

func (ppl *perPathLister) List(path string) (Directory, error) {
	if f, ok := ppl.listFunc[path]; ok {
		return f(path)
	}

	return ppl.defaultLister.List(path)
}

func (ppl *perPathLister) Open(path string) (EntryReadCloser, error) {
	if f, ok := ppl.openFunc[path]; ok {
		return f(path)
	}

	return ppl.defaultLister.Open(path)
}

func (th *uploadTestHarness) cleanup() {
	os.RemoveAll(th.sourceDir)
	os.RemoveAll(th.repoDir)
}

func newUploadTestHarness() *uploadTestHarness {
	sourceDir, err := ioutil.TempDir("", "kopia-src")
	if err != nil {
		panic("cannot create temp directory: " + err.Error())
	}

	repoDir, err := ioutil.TempDir("", "kopia-repo")
	if err != nil {
		panic("cannot create temp directory: " + err.Error())
	}

	storage, err := blob.NewFSStorage(&blob.FSStorageOptions{
		Path: repoDir,
	})

	if err != nil {
		panic("cannot create storage directory: " + err.Error())
	}

	format := cas.Format{
		Version:      "1",
		ObjectFormat: "md5",
	}

	repo, err := cas.NewRepository(storage, format)
	if err != nil {
		panic("unable to create repository: " + err.Error())
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

	th := &uploadTestHarness{
		sourceDir: sourceDir,
		repoDir:   repoDir,
		repo:      repo,
		lister: &perPathLister{
			listFunc:      map[string]func(string) (Directory, error){},
			openFunc:      map[string]func(string) (EntryReadCloser, error){},
			defaultLister: &filesystemLister{},
		},
	}

	th.uploader, err = newUploaderLister(th.repo, th.lister)
	if err != nil {
		panic("can't create uploader: " + err.Error())
	}

	return th
}

func TestUpload(t *testing.T) {
	th := newUploadTestHarness()
	defer th.cleanup()

	var err error

	u, err := NewUploader(th.repo)
	if err != nil {
		t.Errorf("unable to create uploader: %v", err)
		return
	}

	r1, err := u.UploadDir(th.sourceDir, "")
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	r2, err := u.UploadDir(th.sourceDir, r1.ManifestID)
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
	ioutil.WriteFile(filepath.Join(th.sourceDir, "d2/d1/f3"), []byte{1, 2, 3, 4, 5}, 0777)
	r3, err := u.UploadDir(th.sourceDir, r1.ManifestID)
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
	os.Remove(filepath.Join(th.sourceDir, "d2/d1/f3"))

	r4, err := u.UploadDir(th.sourceDir, r1.ManifestID)
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
	r5, err := u.UploadDir(th.sourceDir, r3.ManifestID)
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

func TestUpload_Cancel(t *testing.T) {
}

func TestUpload_TopLevelDirectoryReadFailure(t *testing.T) {
	th := newUploadTestHarness()
	defer th.cleanup()

	th.lister.listFunc[th.sourceDir] = failList

	r, err := th.uploader.UploadDir(th.sourceDir, "")
	if err != errTest {
		t.Errorf("expected error: %v", err)
	}

	if r == nil {
		t.Errorf("result is null")
	}
}

func failList(p string) (Directory, error) {
	return nil, errTest
}

func TestUpload_SubDirectoryReadFailure(t *testing.T) {
	th := newUploadTestHarness()
	defer th.cleanup()

	th.lister.listFunc[filepath.Join(th.sourceDir, "d1")] = failList

	_, err := th.uploader.UploadDir(th.sourceDir, "")
	if err == nil {
		t.Errorf("expected error")
	}
}

func TestUpload_SubdirectoryDeleted(t *testing.T) {
}

func TestUpload_SubdirectoryBecameSymlink(t *testing.T) {
}

func TestUpload_SubdirectoryBecameFile(t *testing.T) {
}

func TestUpload_FileReadFailure(t *testing.T) {
}

func TestUpload_FileUploadFailure(t *testing.T) {
}

func TestUpload_FileDeleted(t *testing.T) {
}

func TestUpload_FileBecameDirectory(t *testing.T) {
}

func TestUpload_FileBecameSymlink(t *testing.T) {
}

func TestUpload_SymlinkDeleted(t *testing.T) {
}

func TestUpload_SymlinkBecameDirectory(t *testing.T) {
}

func TestUpload_SymlinkBecameFile(t *testing.T) {
}
