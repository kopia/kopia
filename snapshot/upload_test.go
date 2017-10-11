package snapshot

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"

	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/object"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/storage"
	"github.com/kopia/kopia/storage/filesystem"

	"testing"

	"github.com/kopia/kopia/auth"
)

type uploadTestHarness struct {
	sourceDir *mockfs.Directory
	repoDir   string
	repo      *repo.Repository
	storage   storage.Storage
}

var errTest = fmt.Errorf("test error")

var progress UploadProgress

func (th *uploadTestHarness) cleanup() {
	os.RemoveAll(th.repoDir)
}

func newUploadTestHarness() *uploadTestHarness {
	ctx := context.Background()
	repoDir, err := ioutil.TempDir("", "kopia-repo")
	if err != nil {
		panic("cannot create temp directory: " + err.Error())
	}

	storage, err := filesystem.New(context.Background(), &filesystem.Options{
		Path: repoDir,
	})

	if err != nil {
		panic("cannot create storage directory: " + err.Error())
	}

	creds, err := auth.Password("foofoofoofoofoofoofoofoo")
	if err != nil {
		panic("unable to create credentials: " + err.Error())
	}

	if err := repo.Initialize(storage, &repo.NewRepositoryOptions{}, creds); err != nil {
		panic("unable to create repository: " + err.Error())
	}

	configFile := filepath.Join(repoDir, ".kopia.config")
	if err := repo.Connect(ctx, configFile, storage, creds, repo.ConnectOptions{
		PersistCredentials: true,
	}); err != nil {
		panic("unable to connect to repository: " + err.Error())
	}

	repo, err := repo.Open(ctx, configFile, nil)
	if err != nil {
		panic("unable to open repository: " + err.Error())
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
	}

	return th
}

func TestUpload(t *testing.T) {
	th := newUploadTestHarness()
	defer th.cleanup()

	u := NewUploader(th.repo)
	s1, err := u.Upload(th.sourceDir, &SourceInfo{}, nil)
	if err != nil {
		t.Errorf("Upload error: %v", err)
	}

	s2, err := u.Upload(th.sourceDir, &SourceInfo{}, s1)
	if err != nil {
		t.Errorf("Upload error: %v", err)
	}

	if !objectIDsEqual(s2.RootObjectID, s1.RootObjectID) {
		t.Errorf("expected s1.RootObjectID==s2.RootObjectID, got %v and %v", s1.RootObjectID.String(), s2.RootObjectID.String())
	}

	if !objectIDsEqual(s2.HashCacheID, s1.HashCacheID) {
		t.Errorf("expected s2.HashCacheID==s1.HashCacheID, got %v and %v", s2.HashCacheID.String(), s1.HashCacheID.String())
	}

	if s1.Stats.CachedFiles != 0 {
		t.Errorf("unexpected s1 stats: %+v", s1.Stats)
	}

	// All non-cached files from s1 are now cached and there are no non-cached files since nothing changed.
	if s2.Stats.CachedFiles != s1.Stats.NonCachedFiles || s2.Stats.NonCachedFiles != 0 {
		t.Errorf("unexpected s2 stats: %+v, vs s1: %+v", s2.Stats, s1.Stats)
	}

	// Add one more file, the s1.RootObjectID should change.
	th.sourceDir.AddFile("d2/d1/f3", []byte{1, 2, 3, 4, 5}, 0777)
	s3, err := u.Upload(th.sourceDir, &SourceInfo{}, s1)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if objectIDsEqual(s2.RootObjectID, s3.RootObjectID) {
		t.Errorf("expected s3.RootObjectID!=s2.RootObjectID, got %v", s3.RootObjectID.String())
	}

	if objectIDsEqual(s2.HashCacheID, s3.HashCacheID) {
		t.Errorf("expected s3.HashCacheID!=s2.HashCacheID, got %v", s3.HashCacheID.String())
	}

	if s3.Stats.NonCachedFiles != 1 {
		// one file is not cached, which causes "./d2/d1/", "./d2/" and "./" to be changed.
		t.Errorf("unexpected s3 stats: %+v", s3.Stats)
	}

	// Now remove the added file, OID should be identical to the original before the file got added.
	th.sourceDir.Subdir("d2", "d1").Remove("f3")

	s4, err := u.Upload(th.sourceDir, &SourceInfo{}, s1)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if !objectIDsEqual(s4.RootObjectID, s1.RootObjectID) {
		t.Errorf("expected s4.RootObjectID==s1.RootObjectID, got %v and %v", s4.RootObjectID, s1.RootObjectID)
	}
	if !objectIDsEqual(s4.HashCacheID, s1.HashCacheID) {
		t.Errorf("expected s4.HashCacheID==s1.HashCacheID, got %v and %v", s4.HashCacheID, s1.HashCacheID)
	}

	// Everything is still cached.
	if s4.Stats.CachedFiles != s1.Stats.NonCachedFiles || s4.Stats.NonCachedFiles != 0 {
		t.Errorf("unexpected s4 stats: %+v", s4.Stats)
	}

	s5, err := u.Upload(th.sourceDir, &SourceInfo{}, s3)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if !objectIDsEqual(s4.RootObjectID, s5.RootObjectID) {
		t.Errorf("expected s4.RootObjectID==s5.RootObjectID, got %v and %v", s4.RootObjectID, s5.RootObjectID)
	}
	if !objectIDsEqual(s4.HashCacheID, s5.HashCacheID) {
		t.Errorf("expected s4.HashCacheID==s5.HashCacheID, got %v and %v", s4.HashCacheID, s5.HashCacheID)
	}

	if s5.Stats.NonCachedFiles != 0 {
		// no files are changed, but one file disappeared which caused "./d2/d1/", "./d2/" and "./" to be changed.
		t.Errorf("unexpected s5 stats: %+v", s5.Stats)
	}
}

func TestUpload_Cancel(t *testing.T) {
}

func TestUpload_TopLevelDirectoryReadFailure(t *testing.T) {
	th := newUploadTestHarness()
	defer th.cleanup()

	th.sourceDir.FailReaddir(errTest)

	u := NewUploader(th.repo)
	s, err := u.Upload(th.sourceDir, &SourceInfo{}, nil)
	if err != errTest {
		t.Errorf("expected error: %v", err)
	}

	if s != nil {
		t.Errorf("result not nil: %v", s)
	}
}

func TestUpload_SubDirectoryReadFailure(t *testing.T) {
	th := newUploadTestHarness()
	defer th.cleanup()

	th.sourceDir.Subdir("d1").FailReaddir(errTest)

	u := NewUploader(th.repo)
	u.IgnoreFileErrors = false
	_, err := u.Upload(th.sourceDir, &SourceInfo{}, nil)
	if err == nil {
		t.Errorf("expected error")
	}
}

func objectIDsEqual(o1 object.ObjectID, o2 object.ObjectID) bool {
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
