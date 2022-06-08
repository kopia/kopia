package snapshotfs

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/fs/virtualfs"
	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/logging"
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
	}, true)
	if err != nil {
		panic("cannot create storage directory: " + err.Error())
	}

	if initerr := repo.Initialize(ctx, storage, &repo.NewRepositoryOptions{}, masterPassword); initerr != nil {
		panic("unable to create repository: " + initerr.Error())
	}

	t.Logf("repo dir: %v", repoDir)

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

	_, w, err := rep.NewWriter(ctx, repo.WriteSessionOptions{Purpose: "test"})
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

	t.Logf("Uploading s1")

	u := NewUploader(th.repo)

	policyTree := policy.BuildTree(nil, policy.DefaultPolicy)

	s1, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{})
	if err != nil {
		t.Errorf("Upload error: %v", err)
	}

	t.Logf("s1: %v", s1.RootEntry)

	t.Logf("Uploading s2")

	s2, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{}, s1)
	if err != nil {
		t.Errorf("Upload error: %v", err)
	}

	if !objectIDsEqual(s2.RootObjectID(), s1.RootObjectID()) {
		t.Errorf("expected s1.RootObjectID==s2.RootObjectID, got %v and %v", s1.RootObjectID().String(), s2.RootObjectID().String())
	}

	if got, want := atomic.LoadInt32(&s1.Stats.CachedFiles), int32(0); got != want {
		t.Errorf("unexpected s1 cached files: %v, want %v", got, want)
	}

	// All non-cached files from s1 are now cached and there are no non-cached files since nothing changed.
	if got, want := atomic.LoadInt32(&s2.Stats.CachedFiles), atomic.LoadInt32(&s1.Stats.NonCachedFiles); got != want {
		t.Errorf("unexpected s2 cached files: %v, want %v", got, want)
	}

	if got, want := atomic.LoadInt32(&s2.Stats.NonCachedFiles), int32(0); got != want {
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

	if atomic.LoadInt32(&s3.Stats.NonCachedFiles) != 1 {
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
	if atomic.LoadInt32(&s4.Stats.CachedFiles) != atomic.LoadInt32(&s1.Stats.NonCachedFiles) || atomic.LoadInt32(&s4.Stats.NonCachedFiles) != 0 {
		t.Errorf("unexpected s4 stats: %+v", s4.Stats)
	}

	s5, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{}, s3)
	if err != nil {
		t.Errorf("upload failed: %v", err)
	}

	if !objectIDsEqual(s4.RootObjectID(), s5.RootObjectID()) {
		t.Errorf("expected s4.RootObjectID==s5.RootObjectID, got %v and %v", s4.RootObjectID(), s5.RootObjectID())
	}

	if atomic.LoadInt32(&s5.Stats.NonCachedFiles) != 0 {
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
	require.ErrorIs(t, err, errTest)
	require.Nil(t, s)
}

func TestUploadDoesNotReportProgressForIgnoredFilesTwice(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)

	defer th.cleanup()

	sourceDir := mockfs.NewDirectory()
	sourceDir.AddFile("f1", []byte{1, 2, 3}, defaultPermissions)
	sourceDir.AddFile("f2", []byte{1, 2, 3, 4}, defaultPermissions)
	sourceDir.AddFile("f3", []byte{1, 2, 3, 4, 5}, defaultPermissions)

	sourceDir.AddDir("d1", defaultPermissions)
	sourceDir.AddFile("d1/f1", []byte{1, 2, 3}, defaultPermissions)

	sourceDir.AddDir("d2", defaultPermissions)
	sourceDir.AddFile("d2/f1", []byte{1, 2, 3}, defaultPermissions)
	sourceDir.AddFile("d2/f2", []byte{1, 2, 3, 4}, defaultPermissions)

	u := NewUploader(th.repo)
	cup := &CountingUploadProgress{}
	u.Progress = cup
	u.OverrideEntryLogDetail = policy.NewLogDetail(10)
	u.OverrideDirLogDetail = policy.NewLogDetail(10)

	policyTree := policy.BuildTree(map[string]*policy.Policy{
		".": {
			FilesPolicy: policy.FilesPolicy{
				IgnoreRules: []string{"d2", "f2"},
			},
		},
	}, policy.DefaultPolicy)

	_, err := u.Upload(ctx, sourceDir, policyTree, snapshot.SourceInfo{})
	require.NoError(t, err)

	// make sure ignored counter is only incremented by 1, even though we process each directory twice
	// - once during estimation and once during upload.
	require.EqualValues(t, 1, cup.counters.TotalExcludedFiles)
	require.EqualValues(t, 1, cup.counters.TotalExcludedDirs)
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
	require.NoError(t, err)

	require.NotEqual(t, "", man.IncompleteReason, "snapshot not marked as incomplete")

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

	trueValue := policy.OptionalBool(true)

	policyTree := policy.BuildTree(nil, &policy.Policy{
		ErrorHandlingPolicy: policy.ErrorHandlingPolicy{
			IgnoreFileErrors:      &trueValue,
			IgnoreDirectoryErrors: &trueValue,
		},
	})

	man, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{})
	require.NoError(t, err)

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

	trueValue := policy.OptionalBool(true)
	falseValue := policy.OptionalBool(false)

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
	require.NoError(t, err)

	// make sure we have 2 errors
	require.Equal(t, 2, man.RootEntry.DirSummary.FatalErrorCount)

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

	require.Equal(t, wantFatalErrors, man.RootEntry.DirSummary.FatalErrorCount, "invalid number of fatal errors")
	require.Equal(t, wantIgnoredErrors, man.RootEntry.DirSummary.IgnoredErrorCount, "invalid number of ignored errors")
	require.Empty(t, pretty.Compare(man.RootEntry.DirSummary.FailedEntries, wantErrors), "unexpected errors, diff(-got,+want)")
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

	trueValue := policy.OptionalBool(true)

	// set up a policy tree where errors from d3 are ignored.
	policyTree := policy.BuildTree(map[string]*policy.Policy{
		"./d3": {
			ErrorHandlingPolicy: policy.ErrorHandlingPolicy{
				IgnoreFileErrors:      &trueValue,
				IgnoreDirectoryErrors: &trueValue,
			},
		},
	}, policy.DefaultPolicy)

	if got, want := policyTree.Child("d3").EffectivePolicy().ErrorHandlingPolicy.IgnoreDirectoryErrors.OrDefault(false), true; got != want {
		t.Fatalf("policy not effective")
	}

	man, err := u.Upload(ctx, th.sourceDir, policyTree, snapshot.SourceInfo{})
	require.NoError(t, err)

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
		d := d

		d.OnReaddir(func() {
			t.Logf("onReadDir %v %s", d.Name(), debug.Stack())
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

	require.Len(t, snapshots, len(dirsToCheckpointAt))

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

	t.Logf("Uploading static directory with streaming file")

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

	staticRoot := virtualfs.NewStaticDirectory("rootdir", []fs.Entry{
		virtualfs.StreamingFileFromReader("stream-file", r),
	})

	man, err := u.Upload(ctx, staticRoot, policyTree, snapshot.SourceInfo{})
	if err != nil {
		t.Fatalf("Upload error: %v", err)
	}

	if got, want := atomic.LoadInt32(&man.Stats.CachedFiles), int32(0); got != want {
		t.Fatalf("unexpected manifest cached files: %v, want %v", got, want)
	}

	if got, want := atomic.LoadInt32(&man.Stats.NonCachedFiles), int32(1); got != want {
		// one file is not cached
		t.Fatalf("unexpected manifest non-cached files: %v, want %v", got, want)
	}

	if got, want := atomic.LoadInt32(&man.Stats.TotalDirectoryCount), int32(1); got != want {
		// must have one directory
		t.Fatalf("unexpected manifest directory count: %v, want %v", got, want)
	}

	if got, want := atomic.LoadInt32(&man.Stats.TotalFileCount), int32(1); got != want {
		// must have one file
		t.Fatalf("unexpected manifest file count: %v, want %v", got, want)
	}
}

func TestUpload_StreamingDirectory(t *testing.T) {
	cases := map[string]bool{
		"MultipleIterations": true,
		"SingleIteration":    false,
	}

	for desc, iterations := range cases {
		t.Run(desc, func(t *testing.T) {
			ctx := testlogging.Context(t)
			th := newUploadTestHarness(ctx, t)

			defer th.cleanup()

			t.Logf("Uploading streaming directory with mock file")

			u := NewUploader(th.repo)

			policyTree := policy.BuildTree(nil, policy.DefaultPolicy)

			files := []fs.Entry{
				mockfs.NewFile("f1", []byte{1, 2, 3}, defaultPermissions),
			}

			staticRoot := virtualfs.NewStaticDirectory("rootdir", []fs.Entry{
				virtualfs.NewStreamingDirectory(
					"stream-directory",
					func(innerCtx context.Context, callback func(context.Context, fs.Entry) error) error {
						for _, f := range files {
							if err := callback(innerCtx, f); err != nil {
								return err
							}
						}

						return nil
					},
					iterations,
				),
			})

			man, err := u.Upload(ctx, staticRoot, policyTree, snapshot.SourceInfo{})
			if err != nil {
				t.Fatalf("Upload error: %v", err)
			}

			if got, want := atomic.LoadInt32(&man.Stats.CachedFiles), int32(0); got != want {
				t.Fatalf("unexpected manifest cached files: %v, want %v", got, want)
			}

			if got, want := atomic.LoadInt32(&man.Stats.NonCachedFiles), int32(1); got != want {
				// one file is not cached
				t.Fatalf("unexpected manifest non-cached files: %v, want %v", got, want)
			}

			if got, want := atomic.LoadInt32(&man.Stats.TotalDirectoryCount), int32(2); got != want {
				// must have one directory
				t.Fatalf("unexpected manifest directory count: %v, want %v", got, want)
			}

			if got, want := atomic.LoadInt32(&man.Stats.TotalFileCount), int32(1); got != want {
				// must have one file
				t.Fatalf("unexpected manifest file count: %v, want %v", got, want)
			}
		})
	}
}

func TestUpload_StreamingDirectoryWithIgnoredFile(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)

	defer th.cleanup()

	t.Logf("Uploading streaming directory with some ignored mock files")

	u := NewUploader(th.repo)

	policyTree := policy.BuildTree(map[string]*policy.Policy{
		".": {
			FilesPolicy: policy.FilesPolicy{
				IgnoreRules: []string{"f2"},
			},
		},
	}, policy.DefaultPolicy)

	files := []fs.Entry{
		mockfs.NewFile("f1", []byte{1, 2, 3}, defaultPermissions),
		mockfs.NewFile("f2", []byte{1, 2, 3, 4}, defaultPermissions),
	}

	staticRoot := virtualfs.NewStaticDirectory("rootdir", []fs.Entry{
		virtualfs.NewStreamingDirectory(
			"stream-directory",
			func(innerCtx context.Context, callback func(context.Context, fs.Entry) error) error {
				for _, f := range files {
					if err := callback(innerCtx, f); err != nil {
						return err
					}
				}

				return nil
			},
			true,
		),
	})

	man, err := u.Upload(ctx, staticRoot, policyTree, snapshot.SourceInfo{})
	if err != nil {
		t.Fatalf("Upload error: %v", err)
	}

	if got, want := atomic.LoadInt32(&man.Stats.CachedFiles), int32(0); got != want {
		t.Fatalf("unexpected manifest cached files: %v, want %v", got, want)
	}

	if got, want := atomic.LoadInt32(&man.Stats.NonCachedFiles), int32(1); got != want {
		// one file is not cached
		t.Fatalf("unexpected manifest non-cached files: %v, want %v", got, want)
	}

	if got, want := atomic.LoadInt32(&man.Stats.TotalDirectoryCount), int32(2); got != want {
		// must have one directory
		t.Fatalf("unexpected manifest directory count: %v, want %v", got, want)
	}

	if got, want := atomic.LoadInt32(&man.Stats.TotalFileCount), int32(1); got != want {
		// must have one file
		t.Fatalf("unexpected manifest file count: %v, want %v", got, want)
	}
}

type mockLogger struct {
	logging.Logger

	logged []loggedAction
}

func (l *mockLogger) Debugw(msg string, keysAndValues ...interface{}) {
	m := map[string]interface{}{}

	for i := 0; i+1 < len(keysAndValues); i += 2 {
		s, ok := keysAndValues[i].(string)
		if !ok {
			panic("not a string key")
		}

		m[s] = keysAndValues[i+1]
	}

	l.logged = append(l.logged, loggedAction{msg, m})
}

func TestParallelUploadDedup(t *testing.T) {
	ctx := testlogging.Context(t)
	th := newUploadTestHarness(ctx, t)

	defer th.cleanup()

	t.Logf("Uploading static directory with streaming file")

	u := NewUploader(th.repo)
	u.ParallelUploads = 10

	pol := *policy.DefaultPolicy
	pol.CompressionPolicy.CompressorName = "pgzip"

	policyTree := policy.BuildTree(nil, &pol)

	testutil.TestSkipOnCIUnlessLinuxAMD64(t)
	td := testutil.TempDirectory(t)

	// 10 identical non-compressible files, 50MB each
	var files []*os.File

	for i := 0; i < 10; i++ {
		f, cerr := os.Create(filepath.Join(td, fmt.Sprintf("file-%v", i)))
		require.NoError(t, cerr)

		files = append(files, f)
	}

	for j := 0; j < 1000; j++ {
		buf := make([]byte, 50000)
		rand.Read(buf)

		for _, f := range files {
			_, werr := f.Write(buf)
			require.NoError(t, werr)
		}
	}

	for _, f := range files {
		f.Close()
	}

	srcdir, err := localfs.Directory(td)
	require.NoError(t, err)

	_, err = u.Upload(ctx, srcdir, policyTree, snapshot.SourceInfo{})
	require.NoError(t, err)

	// we wrote 500 MB, which can be deduped to 50MB, repo size must be less than 51MB
	require.Less(t, testutil.MustGetTotalDirSize(t, th.repoDir), int64(51000000))
}

type loggedAction struct {
	msg           string
	keysAndValues map[string]interface{}
}

// nolint:maintidx
func TestUploadLogging(t *testing.T) {
	sourceDir := mockfs.NewDirectory()
	sourceDir.AddFile("f1", []byte{1, 2, 3}, defaultPermissions)
	sourceDir.AddFile("f2", []byte{1, 2, 3, 4}, defaultPermissions)
	sourceDir.AddFile("f3", []byte{1, 2, 3, 4, 5}, defaultPermissions)
	sourceDir.AddSymlink("f4", "f2", defaultPermissions)
	sourceDir.AddErrorEntry("f5", defaultPermissions, errors.New("some error"))

	sourceDir.AddDir("d1", defaultPermissions)
	sourceDir.AddDir("d1/d3", defaultPermissions)
	sourceDir.AddFile("d1/d3/f1", []byte{1, 2, 3}, defaultPermissions)
	sourceDir.AddFile("d1/d3/f2", []byte{1, 2, 3, 4}, defaultPermissions)
	sourceDir.AddSymlink("d1/d3/f3", "f1", defaultPermissions)

	cases := []struct {
		desc                string
		globalLoggingPolicy *policy.LoggingPolicy
		globalFilesPolicy   *policy.FilesPolicy
		dirLogDetail        *policy.LogDetail
		entryLogDetail      *policy.LogDetail
		wantEntries         []string
		wantEntriesSecond   []string
		wantDetailKeys      map[string][]string
	}{
		{
			desc:           "override-logging disabled",
			dirLogDetail:   policy.NewLogDetail(0),
			entryLogDetail: policy.NewLogDetail(0),
			wantEntries: []string{
				// errors are always logged
				"error f5",
			},
			wantEntriesSecond: []string{
				// errors are always logged
				"error f5",
			},
			wantDetailKeys: map[string][]string{
				"cached": {"dur", "path"},
				"error":  {"error", "path"},
			},
		},
		{
			desc:           "override-minimal logging",
			dirLogDetail:   policy.NewLogDetail(1),
			entryLogDetail: policy.NewLogDetail(1),
			wantEntries: []string{
				"snapshotted file d1/d3/f1",
				"snapshotted file d1/d3/f2",
				"snapshotted symlink d1/d3/f3",
				"snapshotted directory d1/d3",
				"snapshotted directory d1",
				"snapshotted file f1",
				"snapshotted file f2",
				"snapshotted file f3",
				"snapshotted symlink f4",
				"error f5",
				"snapshotted directory .",
			},
			wantEntriesSecond: []string{
				"cached d1/d3/f1",
				"cached d1/d3/f2",
				"cached d1/d3/f3",
				"snapshotted directory d1/d3",
				"snapshotted directory d1",
				"cached f1",
				"cached f2",
				"cached f3",
				"cached f4",
				"error f5",
				"snapshotted directory .",
			},
			// at level 1 only durations and paths are logged
			wantDetailKeys: map[string][]string{
				"cached":                {"dur", "path"},
				"error":                 {"path", "error", "dur"},
				"snapshotted file":      {"path", "dur"},
				"snapshotted directory": {"path", "dur"},
				"snapshotted symlink":   {"path", "dur"},
			},
		},
		// only directories are logged
		{
			desc:           "override-directory-only-logging",
			dirLogDetail:   policy.NewLogDetail(policy.LogDetailMax),
			entryLogDetail: policy.NewLogDetail(0),
			wantEntries: []string{
				"snapshotted directory d1/d3",
				"snapshotted directory d1",
				"error f5",
				"snapshotted directory .",
			},
			wantEntriesSecond: []string{
				"snapshotted directory d1/d3",
				"snapshotted directory d1",
				"error f5",
				"snapshotted directory .",
			},
			// at level 10 a lot of details are logged.
			wantDetailKeys: map[string][]string{
				"error":                 {"path", "error"},
				"snapshotted directory": {"dirs", "dur", "errors", "files", "mtime", "oid", "path", "size"},
			},
		},
		// only entries are scheduled.
		{
			desc:           "override-entry-only-logging",
			dirLogDetail:   policy.NewLogDetail(0),
			entryLogDetail: policy.NewLogDetail(policy.LogDetailMax),
			wantEntries: []string{
				"snapshotted file d1/d3/f1",
				"snapshotted file d1/d3/f2",
				"snapshotted symlink d1/d3/f3",
				"snapshotted file f1",
				"snapshotted file f2",
				"snapshotted file f3",
				"snapshotted symlink f4",
				"error f5",
			},
			wantEntriesSecond: []string{
				"cached d1/d3/f1",
				"cached d1/d3/f2",
				"cached d1/d3/f3",
				"cached f1",
				"cached f2",
				"cached f3",
				"cached f4",
				"error f5",
			},
			// at level 10 a lot of details are logged.
			wantDetailKeys: map[string][]string{
				"cached":              {"dur", "mtime", "oid", "path", "size"},
				"error":               {"dur", "error", "path"},
				"snapshotted file":    {"dur", "mtime", "oid", "path", "size"},
				"snapshotted symlink": {"dur", "mtime", "oid", "path", "size"},
			},
		},
		{
			desc: "default-policy",
			wantDetailKeys: map[string][]string{
				"cached":                {"dur", "mtime", "oid", "path", "size"},
				"error":                 {"error", "path"},
				"snapshotted file":      {"dur", "path", "size"},
				"snapshotted symlink":   {"dur", "path", "size"},
				"snapshotted directory": {"dirs", "dur", "errors", "files", "path", "size"},
			},
			wantEntries: []string{
				"snapshotted directory d1/d3",
				"snapshotted directory d1",
				"error f5",
				"snapshotted directory .",
			},
			// cache hits are not logged
			wantEntriesSecond: []string{
				"snapshotted directory d1/d3",
				"snapshotted directory d1",
				"error f5",
				"snapshotted directory .",
			},
		},
		{
			desc: "global-logging-policy",
			globalLoggingPolicy: &policy.LoggingPolicy{
				Directories: policy.DirLoggingPolicy{
					Snapshotted: policy.NewLogDetail(3),
				},
				Entries: policy.EntryLoggingPolicy{
					Snapshotted: policy.NewLogDetail(3),
				},
			},
			wantDetailKeys: map[string][]string{
				"cached":                {"dur", "mtime", "oid", "path", "size"},
				"error":                 {"dur", "error", "path"},
				"snapshotted file":      {"dur", "path", "size"},
				"snapshotted symlink":   {"dur", "path", "size"},
				"snapshotted directory": {"dur", "path", "size"},
			},
			wantEntries: []string{
				"snapshotted file d1/d3/f1",
				"snapshotted file d1/d3/f2",
				"snapshotted symlink d1/d3/f3",
				"snapshotted directory d1/d3",
				"snapshotted directory d1",
				"snapshotted file f1",
				"snapshotted file f2",
				"snapshotted file f3",
				"snapshotted symlink f4",
				"error f5",
				"snapshotted directory .",
			},
			// cache hits are not logged
			wantEntriesSecond: []string{
				"snapshotted directory d1/d3",
				"snapshotted directory d1",
				"error f5",
				"snapshotted directory .",
			},
		},
		{
			desc: "complex-logging-policy",
			globalLoggingPolicy: &policy.LoggingPolicy{
				Directories: policy.DirLoggingPolicy{
					Snapshotted: policy.NewLogDetail(3),
					Ignored:     policy.NewLogDetail(4),
				},
				Entries: policy.EntryLoggingPolicy{
					Ignored:     policy.NewLogDetail(3),
					Snapshotted: policy.NewLogDetail(3),
					CacheMiss:   policy.NewLogDetail(4),
					CacheHit:    policy.NewLogDetail(5),
				},
			},
			globalFilesPolicy: &policy.FilesPolicy{
				IgnoreRules: []string{"f1", "d3"},
			},
			wantDetailKeys: map[string][]string{
				"cache miss":            {"mode", "mtime", "path", "size"},
				"cached":                {"dur", "path", "size"},
				"error":                 {"dur", "error", "path"},
				"snapshotted file":      {"dur", "path", "size"},
				"snapshotted symlink":   {"dur", "path", "size"},
				"snapshotted directory": {"dur", "path", "size"},
				"ignored directory":     {"dur", "path"},
				"ignored":               {"dur", "path"},
			},
			wantEntries: []string{
				"ignored directory d1/d3",
				"snapshotted directory d1",
				"ignored f1",
				"snapshotted file f2",
				"snapshotted file f3",
				"snapshotted symlink f4",
				"error f5",
				"snapshotted directory .",
			},
			wantEntriesSecond: []string{
				"ignored directory d1/d3",
				"snapshotted directory d1",
				"ignored f1",
				"cached f2",
				"cached f3",
				"cached f4",
				"error f5",
				"snapshotted directory .",
			},
		},
	}

	sourceInfo := snapshot.SourceInfo{
		Host:     "somehost",
		UserName: "someuser",
		Path:     "/somepath",
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			ml := &mockLogger{
				Logger: logging.NullLogger(),
			}

			ctx := testlogging.Context(t)
			ctx = logging.WithLogger(ctx, func(module string) logging.Logger {
				if module == "uploader" {
					// only capture logs from the uploader, not the estimator.
					return ml
				}

				return logging.NullLogger()
			})
			th := newUploadTestHarness(ctx, t)

			defer th.cleanup()

			u := NewUploader(th.repo)

			// make sure uploads are strictly sequential to get predictable log output.
			u.ParallelUploads = 1

			pol := *policy.DefaultPolicy
			if p := tc.globalLoggingPolicy; p != nil {
				pol.LoggingPolicy = *p
			}

			if p := tc.globalFilesPolicy; p != nil {
				pol.FilesPolicy = *p
			}

			policy.SetPolicy(ctx, th.repo, policy.GlobalPolicySourceInfo, &pol)

			u.OverrideDirLogDetail = tc.dirLogDetail
			u.OverrideEntryLogDetail = tc.entryLogDetail

			ml.logged = nil

			polTree, err := policy.TreeForSource(ctx, th.repo, sourceInfo)
			require.NoError(t, err)

			man1, err := u.Upload(ctx, sourceDir, polTree, sourceInfo)
			require.NoError(t, err)

			var gotEntries []string

			for _, l := range ml.logged {
				gotEntries = append(gotEntries, fmt.Sprintf("%v %v", l.msg, l.keysAndValues["path"]))

				require.Contains(t, tc.wantDetailKeys, l.msg)
				verifyLogDetails(t, l.msg, tc.wantDetailKeys[l.msg], l.keysAndValues)
			}

			require.Equal(t, tc.wantEntries, gotEntries)

			ml.logged = nil

			// run second upload with previous manifest to trigger cache.
			_, err = u.Upload(ctx, sourceDir, polTree, sourceInfo, man1)
			require.NoError(t, err)

			var gotEntriesSecond []string

			for _, l := range ml.logged {
				gotEntriesSecond = append(gotEntriesSecond, fmt.Sprintf("%v %v", l.msg, l.keysAndValues["path"]))

				require.Contains(t, tc.wantDetailKeys, l.msg)
				verifyLogDetails(t, "second "+l.msg, tc.wantDetailKeys[l.msg], l.keysAndValues)
			}

			require.Equal(t, tc.wantEntriesSecond, gotEntriesSecond)
		})
	}
}

func verifyLogDetails(t *testing.T, desc string, wantDetailKeys []string, keysAndValues map[string]interface{}) {
	t.Helper()

	var gotDetailKeys []string

	for k := range keysAndValues {
		gotDetailKeys = append(gotDetailKeys, k)
	}

	sort.Strings(gotDetailKeys)
	sort.Strings(wantDetailKeys)
	require.Equal(t, wantDetailKeys, gotDetailKeys, "invalid details for "+desc)
}
