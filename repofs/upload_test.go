package repofs

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"

	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/storage"
	"github.com/kopia/kopia/storage/filesystem"

	"testing"
)

type uploadTestHarness struct {
	sourceDir *mockfs.Directory
	repoDir   string
	repo      *repo.Repository
	storage   storage.Storage
	uploader  Uploader
}

var errTest = fmt.Errorf("test error")

func (th *uploadTestHarness) cleanup() {
	os.RemoveAll(th.repoDir)
}

func newUploadTestHarness() *uploadTestHarness {
	repoDir, err := ioutil.TempDir("", "kopia-repo")
	if err != nil {
		panic("cannot create temp directory: " + err.Error())
	}

	storage, err := filesystem.New(&filesystem.Options{
		Path: repoDir,
	})

	if err != nil {
		panic("cannot create storage directory: " + err.Error())
	}

	format := repo.Format{
		Version:                1,
		ObjectFormat:           "TESTONLY_MD5",
		MaxBlockSize:           1000,
		MaxInlineContentLength: 0,
	}

	repo, err := repo.New(storage, &format)
	if err != nil {
		panic("unable to create repository: " + err.Error())
	}

	sourceDir := mockfs.NewDirectory()
	sourceDir.AddFile("f1", []byte{1, 2, 3}, 0777)
	sourceDir.AddFile("f2", []byte{1, 2, 3, 4}, 0777)
	sourceDir.AddFile("f3", []byte{1, 2, 3, 4, 5}, 0777)

	sourceDir.AddDir("d1", 0777)
	sourceDir.AddDir("d1/d1", 0777)
	sourceDir.AddDir("d1/d2", 0777)
	sourceDir.AddDir("d2", 0777)
	sourceDir.AddDir("d2/d1", 0777)

	// Prepare directory contents.
	sourceDir.AddFile("d1/d1/f1", []byte{1, 2, 3}, 0777)
	sourceDir.AddFile("d1/d1/f2", []byte{1, 2, 3, 4}, 0777)
	sourceDir.AddFile("d1/f2", []byte{1, 2, 3, 4}, 0777)
	sourceDir.AddFile("d1/d2/f1", []byte{1, 2, 3}, 0777)
	sourceDir.AddFile("d1/d2/f2", []byte{1, 2, 3, 4}, 0777)
	sourceDir.AddFile("d2/d1/f1", []byte{1, 2, 3}, 0777)
	sourceDir.AddFile("d2/d1/f2", []byte{1, 2, 3, 4}, 0777)

	th := &uploadTestHarness{
		sourceDir: sourceDir,
		repoDir:   repoDir,
		repo:      repo,
		uploader:  NewUploader(repo),
	}

	return th
}

func TestUpload(t *testing.T) {
	th := newUploadTestHarness()
	defer th.cleanup()

	r1, err := th.uploader.UploadDir(th.sourceDir, nil)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	log.Printf("--------------------------")
	r2, err := th.uploader.UploadDir(th.sourceDir, &r1.ManifestID)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}
	log.Printf("--------------------------")

	if !objectIDsEqual(r2.ObjectID, r1.ObjectID) {
		t.Errorf("expected r1.ObjectID==r2.ObjectID, got %v and %v", r1.ObjectID.UIString(), r2.ObjectID.UIString())
	}

	if !objectIDsEqual(r2.ManifestID, r1.ManifestID) {
		t.Errorf("expected r2.ManifestID==r1.ManifestID, got %v and %v", r2.ManifestID.UIString(), r1.ManifestID.UIString())
	}

	if r1.Stats.CachedFiles+r1.Stats.CachedDirectories != 0 {
		t.Errorf("unexpected r1 stats: %+v", r1.Stats)
	}

	// All non-cached files from r1 are now cached and there are no non-cached files or dirs since nothing changed.
	if r2.Stats.CachedFiles+r2.Stats.CachedDirectories != r1.Stats.NonCachedFiles+r1.Stats.NonCachedDirectories ||
		r2.Stats.NonCachedFiles+r2.Stats.NonCachedDirectories != 0 {
		t.Errorf("unexpected r2 stats: %+v, vs r1: %+v", r2.Stats, r1.Stats)
	}

	// Add one more file, the r1.ObjectID should change.
	th.sourceDir.AddFile("d2/d1/f3", []byte{1, 2, 3, 4, 5}, 0777)
	r3, err := th.uploader.UploadDir(th.sourceDir, &r1.ManifestID)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if objectIDsEqual(r2.ObjectID, r3.ObjectID) {
		t.Errorf("expected r3.ObjectID!=r2.ObjectID, got %v", r3.ObjectID.UIString())
	}

	if objectIDsEqual(r2.ManifestID, r3.ManifestID) {
		t.Errorf("expected r3.ManifestID!=r2.ManifestID, got %v", r3.ManifestID.UIString())
	}

	if r3.Stats.NonCachedFiles != 1 && r3.Stats.NonCachedDirectories != 3 {
		// one file is not cached, which causes "./d2/d1/", "./d2/" and "./" to be changed.
		t.Errorf("unexpected r3 stats: %+v", r3.Stats)
	}

	// Now remove the added file, OID should be identical to the original before the file got added.
	th.sourceDir.Subdir("d2", "d1").Remove("f3")

	r4, err := th.uploader.UploadDir(th.sourceDir, &r1.ManifestID)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if !objectIDsEqual(r4.ObjectID, r1.ObjectID) {
		t.Errorf("expected r4.ObjectID==r1.ObjectID, got %v and %v", r4.ObjectID, r1.ObjectID)
	}
	if !objectIDsEqual(r4.ManifestID, r1.ManifestID) {
		t.Errorf("expected r4.ManifestID==r1.ManifestID, got %v and %v", r4.ManifestID, r1.ManifestID)
	}

	// Everything is still cached.
	if r4.Stats.CachedFiles+r4.Stats.CachedDirectories != r1.Stats.NonCachedFiles+r1.Stats.NonCachedDirectories ||
		r4.Stats.NonCachedFiles+r4.Stats.NonCachedDirectories != 0 {
		t.Errorf("unexpected r4 stats: %+v", r4.Stats)
	}

	// Upload again, this time using r3.ManifestID as base.
	r5, err := th.uploader.UploadDir(th.sourceDir, &r3.ManifestID)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if r5.Stats.NonCachedFiles != 0 && r5.Stats.NonCachedDirectories != 3 {
		// no files are changed, but one file disappeared which caused "./d2/d1/", "./d2/" and "./" to be changed.
		t.Errorf("unexpected r5 stats: %+v", r5.Stats)
	}
}

func TestUpload_Cancel(t *testing.T) {
}

func TestUpload_TopLevelDirectoryReadFailure(t *testing.T) {
	th := newUploadTestHarness()
	defer th.cleanup()

	th.sourceDir.FailReaddir(errTest)

	r, err := th.uploader.UploadDir(th.sourceDir, nil)
	if err != errTest {
		t.Errorf("expected error: %v", err)
	}

	if r == nil {
		t.Errorf("result is null")
	}
}

func TestUpload_SubDirectoryReadFailure(t *testing.T) {
	th := newUploadTestHarness()
	defer th.cleanup()

	th.sourceDir.Subdir("d1").FailReaddir(errTest)

	_, err := th.uploader.UploadDir(th.sourceDir, nil)
	if err == nil {
		t.Errorf("expected error")
	}
}

func objectIDsEqual(o1 repo.ObjectID, o2 repo.ObjectID) bool {
	return reflect.DeepEqual(o1, o2)
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
