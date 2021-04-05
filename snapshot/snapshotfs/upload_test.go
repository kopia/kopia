package snapshotfs

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/virtualfs"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
)

const (
	masterPassword     = "foofoofoofoofoofoofoofoo"
	defaultPermissions = 0o777
)

type uploadTestHarness struct {
	sourceDir *mockfs.Directory
	repoDir   string
	repo      repo.RepositoryWriter
	ft        *faketime.TimeAdvance
}

var errTest = errors.New("test error")

func (th *uploadTestHarness) cleanup() {
	os.RemoveAll(th.repoDir)
}

func newUploadTestHarness(ctx context.Context, t *testing.T) *uploadTestHarness {
	t.Helper()

	repoDir := testutil.TempDirectory(t)

	storage, err := filesystem.New(ctx, &filesystem.Options{
		Path: repoDir,
	})
	if err != nil {
		panic("cannot create storage directory: " + err.Error())
	}

	if initerr := repo.Initialize(ctx, storage, &repo.NewRepositoryOptions{}, masterPassword); initerr != nil {
		panic("unable to create repository: " + initerr.Error())
	}

	log(ctx).Debugf("repo dir: %v", repoDir)

	configFile := filepath.Join(repoDir, ".kopia.config")
	if conerr := repo.Connect(ctx, configFile, storage, masterPassword, nil); conerr != nil {
		panic("unable to connect to repository: " + conerr.Error())
	}

	ft := faketime.NewTimeAdvance(time.Date(2018, time.February, 6, 0, 0, 0, 0, time.UTC), 0)

	rep, err := repo.Open(ctx, configFile, masterPassword, &repo.Options{
		TimeNowFunc: ft.NowFunc(),
	})
	if err != nil {
		panic("unable to open repository: " + err.Error())
	}

	sourceDir := mockfs.NewDirectory()
	sourceDir.AddFile("f1", []byte{1, 2, 3}, defaultPermissions)
	sourceDir.AddFile("f2", []byte{1, 2, 3, 4}, defaultPermissions)
	sourceDir.AddFile("f3", []byte{1, 2, 3, 4, 5}, defaultPermissions)

	sourceDir.AddDir("d1", defaultPermissions)
	sourceDir.AddDir("d1/d1", defaultPermissions)
	sourceDir.AddDir("d1/d2", defaultPermissions)
	sourceDir.AddDir("d2", defaultPermissions)
	sourceDir.AddDir("d2/d1", defaultPermissions)

	// Prepare directory contents.
	sourceDir.AddFile("d1/d1/f1", []byte{1, 2, 3}, defaultPermissions)
	sourceDir.AddFile("d1/d1/f2", []byte{1, 2, 3, 4}, defaultPermissions)
	sourceDir.AddFile("d1/f2", []byte{1, 2, 3, 4}, defaultPermissions)
	sourceDir.AddFile("d1/d2/f1", []byte{1, 2, 3}, defaultPermissions)
	sourceDir.AddFile("d1/d2/f2", []byte{1, 2, 3, 4}, defaultPermissions)
	sourceDir.AddFile("d2/d1/f1", []byte{1, 2, 3}, defaultPermissions)
	sourceDir.AddFile("d2/d1/f2", []byte{1, 2, 3, 4}, defaultPermissions)

	w, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
	if err != nil {
		panic("writer creation error: " + err.Error())
	}

	th := &uploadTestHarness{
		sourceDir: sourceDir,
		repoDir:   repoDir,
		repo:      w,
		ft:        ft,
	}

	return th
}

// nolint:gocyclo
func TestUpload(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)

	defer th.cleanup()

	log(ctx).Infof("Uploading s1")

	u := NewUploader(th.repo)

	policyTree := policy.BuildTree(nil, policy.DefaultPolicy)

	s1, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{})
	if err != nil {
		t.Errorf("Upload error: %v", err)
	}

	log(ctx).Infof("s1: %v", s1.RootEntry)

	log(ctx).Infof("Uploading s2")

	s2, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{}, s1)
	if err != nil {
		t.Errorf("Upload error: %v", err)
	}

	if !objectIDsEqual(s2.RootObjectID(), s1.RootObjectID()) {
		t.Errorf("expected s1.RootObjectID==s2.RootObjectID, got %v and %v", s1.RootObjectID().String(), s2.RootObjectID().String())
	}

	if got, want := s1.Stats.CachedFiles, int32(0); got != want {
		t.Errorf("unexpected s1 cached files: %v, want %v", got, want)
	}

	// All non-cached files from s1 are now cached and there are no non-cached files since nothing changed.
	if got, want := s2.Stats.CachedFiles, s1.Stats.NonCachedFiles; got != want {
		t.Errorf("unexpected s2 cached files: %v, want %v", got, want)
	}

	if got, want := s2.Stats.NonCachedFiles, int32(0); got != want {
		t.Errorf("unexpected non-cached files: %v", got)
	}

	// Add one more file, the s1.RootObjectID should change.
	th.sourceDir.AddFile("d2/d1/f3", []byte{1, 2, 3, 4, 5}, defaultPermissions)

	s3, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{}, s1)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if objectIDsEqual(s2.RootObjectID(), s3.RootObjectID()) {
		t.Errorf("expected s3.RootObjectID!=s2.RootObjectID, got %v", s3.RootObjectID().String())
	}

	if s3.Stats.NonCachedFiles != 1 {
		// one file is not cached, which causes "./d2/d1/", "./d2/" and "./" to be changed.
		t.Errorf("unexpected s3 stats: %+v", s3.Stats)
	}

	// Now remove the added file, OID should be identical to the original before the file got added.
	th.sourceDir.Subdir("d2", "d1").Remove("f3")

	s4, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{}, s1)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if !objectIDsEqual(s4.RootObjectID(), s1.RootObjectID()) {
		t.Errorf("expected s4.RootObjectID==s1.RootObjectID, got %v and %v", s4.RootObjectID(), s1.RootObjectID())
	}

	// Everything is still cached.
	if s4.Stats.CachedFiles != s1.Stats.NonCachedFiles || s4.Stats.NonCachedFiles != 0 {
		t.Errorf("unexpected s4 stats: %+v", s4.Stats)
	}

	s5, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{}, s3)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if !objectIDsEqual(s4.RootObjectID(), s5.RootObjectID()) {
		t.Errorf("expected s4.RootObjectID==s5.RootObjectID, got %v and %v", s4.RootObjectID(), s5.RootObjectID())
	}

	if s5.Stats.NonCachedFiles != 0 {
		// no files are changed, but one file disappeared which caused "./d2/d1/", "./d2/" and "./" to be changed.
		t.Errorf("unexpected s5 stats: %+v", s5.Stats)
	}
}

func TestUpload_TopLevelDirectoryReadFailure(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)

	defer th.cleanup()

	th.sourceDir.FailReaddir(errTest)

	u := NewUploader(th.repo)

	policyTree := policy.BuildTree(nil, policy.DefaultPolicy)

	s, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{})
	if err.Error() != errTest.Error() {
		t.Errorf("expected error: %v", err)
	}

	if s != nil {
		t.Errorf("result not nil: %v", s)
	}
}

func TestUpload_SubDirectoryReadFailureFailFast(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)

	defer th.cleanup()

	th.sourceDir.Subdir("d1").FailReaddir(errTest)
	th.sourceDir.Subdir("d2").Subdir("d1").FailReaddir(errTest)

	u := NewUploader(th.repo)
	u.ParallelUploads = 1
	u.FailFast = true

	policyTree := policy.BuildTree(nil, policy.DefaultPolicy)

	man, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if man.IncompleteReason == "" {
		t.Fatalf("snapshot not marked as incomplete")
	}

	// will have one error because we're canceling early.
	verifyErrors(t, man, 1, 0,
		[]*fs.EntryWithError{
			{EntryPath: "d1", Error: errTest.Error()},
		},
	)
}

func objectIDsEqual(o1, o2 object.ID) bool {
	return reflect.DeepEqual(o1, o2)
}

func TestUpload_SubDirectoryReadFailureIgnoredNoFailFast(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)

	defer th.cleanup()

	th.sourceDir.Subdir("d1").FailReaddir(errTest)
	th.sourceDir.Subdir("d2").Subdir("d1").FailReaddir(errTest)

	u := NewUploader(th.repo)

	trueValue := true

	policyTree := policy.BuildTree(nil, &policy.Policy{
		ErrorHandlingPolicy: policy.ErrorHandlingPolicy{
			IgnoreFileErrors:      &trueValue,
			IgnoreDirectoryErrors: &trueValue,
		},
	})

	man, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 0 failed, 2 ignored
	verifyErrors(t, man, 0, 2,
		[]*fs.EntryWithError{
			{EntryPath: "d1", Error: errTest.Error()},
			{EntryPath: "d2/d1", Error: errTest.Error()},
		},
	)
}

func TestUpload_ErrorEntries(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)

	defer th.cleanup()

	th.sourceDir.Subdir("d1").AddErrorEntry("some-unknown-entry", os.ModeIrregular, fs.ErrUnknown)
	th.sourceDir.Subdir("d1").AddErrorEntry("some-failed-entry", 0, errors.Errorf("some-other-error"))
	th.sourceDir.Subdir("d2").AddErrorEntry("another-failed-entry", os.ModeIrregular, errors.Errorf("another-error"))

	trueValue := true
	falseValue := false

	cases := []struct {
		desc              string
		rootEntry         fs.Entry
		ehp               policy.ErrorHandlingPolicy
		wantFatalErrors   int
		wantIgnoredErrors int
	}{
		{
			desc:              "default ignore rules",
			rootEntry:         th.sourceDir,
			ehp:               policy.ErrorHandlingPolicy{},
			wantFatalErrors:   2,
			wantIgnoredErrors: 1,
		},
		{
			desc:      "ignore both unknown types and other errors",
			rootEntry: th.sourceDir,
			ehp: policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      &trueValue,
				IgnoreDirectoryErrors: &trueValue,
				IgnoreUnknownTypes:    &trueValue,
			},
			wantFatalErrors:   0,
			wantIgnoredErrors: 3,
		},
		{
			desc:      "ignore no errors",
			rootEntry: th.sourceDir,
			ehp: policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      &falseValue,
				IgnoreDirectoryErrors: &falseValue,
				IgnoreUnknownTypes:    &falseValue,
			},
			wantFatalErrors:   3,
			wantIgnoredErrors: 0,
		},
		{
			desc:      "ignore unknown type errors",
			rootEntry: th.sourceDir,
			ehp: policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      &falseValue,
				IgnoreDirectoryErrors: &falseValue,
				IgnoreUnknownTypes:    &trueValue,
			},
			wantFatalErrors:   2,
			wantIgnoredErrors: 1,
		},
		{
			desc:      "ignore errors except unknown type errors",
			rootEntry: th.sourceDir,
			ehp: policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      &trueValue,
				IgnoreDirectoryErrors: &trueValue,
				IgnoreUnknownTypes:    &falseValue,
			},
			wantFatalErrors:   1,
			wantIgnoredErrors: 2,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			u := NewUploader(th.repo)

			policyTree := policy.BuildTree(nil, &policy.Policy{
				ErrorHandlingPolicy: tc.ehp,
			})

			man, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{})
			if err != nil {
				t.Fatal(err)
			}

			verifyErrors(t, man, tc.wantFatalErrors, tc.wantIgnoredErrors, []*fs.EntryWithError{
				{EntryPath: "d1/some-failed-entry", Error: "some-other-error"},
				{EntryPath: "d1/some-unknown-entry", Error: "unknown or unsupported entry type"},
				{EntryPath: "d2/another-failed-entry", Error: "another-error"},
			})
		})
	}
}

func TestUpload_SubDirectoryReadFailureNoFailFast(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)

	defer th.cleanup()

	th.sourceDir.Subdir("d1").FailReaddir(errTest)
	th.sourceDir.Subdir("d2").Subdir("d1").FailReaddir(errTest)

	u := NewUploader(th.repo)

	policyTree := policy.BuildTree(nil, policy.DefaultPolicy)

	man, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// make sure we have 2 errors
	if got, want := man.RootEntry.DirSummary.FatalErrorCount, 2; got != want {
		t.Errorf("invalid number of failed entries: %v, want %v", got, want)
	}

	verifyErrors(t, man,
		2, 0,
		[]*fs.EntryWithError{
			{EntryPath: "d1", Error: errTest.Error()},
			{EntryPath: "d2/d1", Error: errTest.Error()},
		},
	)
}

func verifyErrors(t *testing.T, man *snapshot.Manifest, wantFatalErrors, wantIgnoredErrors int, wantErrors []*fs.EntryWithError) {
	t.Helper()

	if got, want := man.RootEntry.DirSummary.FatalErrorCount, wantFatalErrors; got != want {
		t.Fatalf("invalid number of fatal errors: %v, want %v", got, want)
	}

	if got, want := man.RootEntry.DirSummary.IgnoredErrorCount, wantIgnoredErrors; got != want {
		t.Fatalf("invalid number of ignored errors: %v, want %v", got, want)
	}

	if diff := pretty.Compare(man.RootEntry.DirSummary.FailedEntries, wantErrors); diff != "" {
		t.Errorf("unexpected errors, diff(-got,+want): %v\n", diff)
	}
}

func TestUpload_SubDirectoryReadFailureSomeIgnoredNoFailFast(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)

	defer th.cleanup()

	th.sourceDir.Subdir("d1").FailReaddir(errTest)
	th.sourceDir.Subdir("d2").Subdir("d1").FailReaddir(errTest)
	th.sourceDir.AddDir("d3", defaultPermissions)
	th.sourceDir.Subdir("d3").FailReaddir(errTest)

	u := NewUploader(th.repo)

	trueValue := true

	// set up a policy tree where errors from d3 are ignored.
	policyTree := policy.BuildTree(map[string]*policy.Policy{
		"./d3": {
			ErrorHandlingPolicy: policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      &trueValue,
				IgnoreDirectoryErrors: &trueValue,
			},
		},
	}, policy.DefaultPolicy)

	if got, want := policyTree.Child("d3").EffectivePolicy().ErrorHandlingPolicy.IgnoreDirectoryErrorsOrDefault(false), true; got != want {
		t.Fatalf("policy not effective")
	}

	man, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	verifyErrors(t, man,
		2, 1,
		[]*fs.EntryWithError{
			{EntryPath: "d1", Error: errTest.Error()},
			{EntryPath: "d2/d1", Error: errTest.Error()},
			{EntryPath: "d3", Error: errTest.Error()},
		},
	)
}

func TestUploadWithCheckpointing(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)

	defer th.cleanup()

	u := NewUploader(th.repo)

	fakeTicker := make(chan time.Time)

	// inject fake ticker that we can control externally instead of through time passage.
	u.getTicker = func(d time.Duration) <-chan time.Time {
		return fakeTicker
	}

	// create a channel that will be sent to whenever checkpoint completes.
	u.checkpointFinished = make(chan struct{})
	u.disableEstimation = true

	policyTree := policy.BuildTree(nil, policy.DefaultPolicy)

	si := snapshot.SourceInfo{
		UserName: "user",
		Host:     "host",
		Path:     "path",
	}

	// inject a action into mock filesystem to trigger and wait for checkpoints at few places.
	// the places are not important, what's important that those are 3 separate points in time.
	dirsToCheckpointAt := []*mockfs.Directory{
		th.sourceDir.Subdir("d1"),
		th.sourceDir.Subdir("d2"),
		th.sourceDir.Subdir("d1").Subdir("d2"),
	}

	for _, d := range dirsToCheckpointAt {
		d.OnReaddir(func() {
			// trigger checkpoint
			fakeTicker <- clock.Now()
			// wait for checkpoint
			<-u.checkpointFinished
		})
	}

	if _, err := u.Upload(ctx, th.sourceDir, policyTree, si); err != nil {
		t.Errorf("Upload error: %v", err)
	}

	snapshots, err := snapshot.ListSnapshots(ctx, th.repo, si)
	if err != nil {
		t.Fatalf("error listing snapshots: %v", err)
	}

	if got, want := len(snapshots), len(dirsToCheckpointAt); got != want {
		t.Fatalf("unexpected number of snapshots: %v, want %v", got, want)
	}

	for _, sn := range snapshots {
		if got, want := sn.IncompleteReason, IncompleteReasonCheckpoint; got != want {
			t.Errorf("unexpected incompleteReason %q, want %q", got, want)
		}
	}
}

func TestUploadScanStopsOnContextCancel(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)

	defer th.cleanup()

	u := NewUploader(th.repo)

	scanctx, cancel := context.WithCancel(ctx)

	th.sourceDir.Subdir("d1").Subdir("d2").OnReaddir(func() {
		cancel()
	})

	result, err := u.scanDirectory(scanctx, th.sourceDir, nil)
	if !errors.Is(err, scanctx.Err()) {
		t.Fatalf("invalid scan error: %v", err)
	}

	if result.numFiles == 0 && result.totalFileSize == 0 {
		t.Fatalf("should have returned partial results, got zeros")
	}
}

func TestUploadScanIgnoresFiles(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)

	defer th.cleanup()

	u := NewUploader(th.repo)

	// set up a policy tree where that ignores some files.
	policyTree := policy.BuildTree(map[string]*policy.Policy{
		".": {
			FilesPolicy: policy.FilesPolicy{
				IgnoreRules: []string{"f1"},
			},
		},
	}, policy.DefaultPolicy)

	// no policy
	result1, err := u.scanDirectory(ctx, th.sourceDir, nil)
	require.NoError(t, err)

	result2, err := u.scanDirectory(ctx, th.sourceDir, policyTree)
	require.NoError(t, err)

	if result1.numFiles == 0 {
		t.Fatalf("no files scanned")
	}

	if result2.numFiles == 0 {
		t.Fatalf("no files scanned")
	}

	if got, want := result2.numFiles, result1.numFiles; got >= want {
		t.Fatalf("expected lower number of files %v, wanted %v", got, want)
	}

	if got, want := result2.totalFileSize, result1.totalFileSize; got >= want {
		t.Fatalf("expected lower file size %v, wanted %v", got, want)
	}
}

func TestUpload_VirtualDirectoryWithStreamingFile(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)

	defer th.cleanup()

	log(ctx).Infof("Uploading static directory with streaming file")

	u := NewUploader(th.repo)

	policyTree := policy.BuildTree(nil, policy.DefaultPolicy)

	// Create a temporary pipe file with test data
	content := []byte("Streaming Temporary file content")

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("error creating pipe file: %v", err)
	}

	if _, err = w.Write(content); err != nil {
		t.Fatalf("error writing to pipe file: %v", err)
	}

	w.Close()

	staticRoot := virtualfs.NewStaticDirectory("rootdir", fs.Entries{
		virtualfs.StreamingFileFromReader("stream-file", r),
	})

	man, err := u.Upload(ctx, staticRoot, policyTree, snapshot.SourceInfo{})
	if err != nil {
		t.Fatalf("Upload error: %v", err)
	}

	if got, want := man.Stats.CachedFiles, int32(0); got != want {
		t.Fatalf("unexpected manifest cached files: %v, want %v", got, want)
	}

	if got, want := man.Stats.NonCachedFiles, int32(1); got != want {
		// one file is not cached
		t.Fatalf("unexpected manifest non-cached files: %v, want %v", got, want)
	}

	if got, want := man.Stats.TotalDirectoryCount, int32(1); got != want {
		// must have one directory
		t.Fatalf("unexpected manifest directory count: %v, want %v", got, want)
	}

	if got, want := man.Stats.TotalFileCount, int32(1); got != want {
		// must have one file
		t.Fatalf("unexpected manifest file count: %v, want %v", got, want)
	}
}
