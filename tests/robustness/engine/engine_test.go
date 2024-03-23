//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

// Package engine provides the framework for a snapshot repository testing engine
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/robustness/fiofilewriter"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/fswalker"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

var (
	repoBaseDirName    = "engine"
	fsBasePath         = "/tmp"
	s3BasePath         = ""
	dataRepoPath       = "unit-tests/data-repo"
	metadataRepoPath   = "unit-tests/metadata-repo"
	fsRepoBaseDirPath  = filepath.Join(fsBasePath, repoBaseDirName)
	s3RepoBaseDirPath  = filepath.Join(s3BasePath, repoBaseDirName)
	fsMetadataRepoPath = filepath.Join(fsRepoBaseDirPath, metadataRepoPath)
	s3MetadataRepoPath = filepath.Join(s3RepoBaseDirPath, metadataRepoPath)
	fsDataRepoPath     = filepath.Join(fsRepoBaseDirPath, dataRepoPath)
	s3DataRepoPath     = filepath.Join(s3RepoBaseDirPath, dataRepoPath)
)

func TestEngineWritefilesBasicFS(t *testing.T) {
	t.Setenv(snapmeta.EngineModeEnvKey, snapmeta.EngineModeBasic)
	t.Setenv(snapmeta.S3BucketNameEnvKey, "")

	ctx := testlogging.Context(t)

	th, eng, err := newTestHarness(ctx, t, fsDataRepoPath, fsMetadataRepoPath)
	if errors.Is(err, kopiarunner.ErrExeVariableNotSet) || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	require.NoError(t, err)

	defer func() {
		cleanupErr := th.Cleanup(ctx)
		require.NoError(t, cleanupErr)

		os.RemoveAll(fsRepoBaseDirPath)
	}()

	opts := map[string]string{}
	err = eng.Init(ctx)
	require.NoError(t, err)

	fileSize := int64(256 * 1024)
	numFiles := 10

	fioOpts := fio.Options{}.WithFileSize(fileSize).WithNumFiles(numFiles)
	fioRunner := th.FioRunner()
	err = fioRunner.WriteFiles("", fioOpts)
	require.NoError(t, err)

	snapIDs := eng.Checker.GetSnapIDs()

	snapID, err := eng.Checker.TakeSnapshot(ctx, fioRunner.LocalDataDir, opts)
	require.NoError(t, err)

	err = eng.Checker.RestoreSnapshot(ctx, snapID, os.Stdout, opts)
	require.NoError(t, err)

	for _, sID := range snapIDs {
		err = eng.Checker.RestoreSnapshot(ctx, sID, os.Stdout, opts)
		require.NoError(t, err)
	}
}

func randomString(n int) string {
	b := make([]byte, n)
	io.ReadFull(rand.Reader, b)

	return hex.EncodeToString(b)
}

func makeTempS3Bucket(t *testing.T) (bucketName string, cleanupCB func()) {
	t.Helper()

	endpoint := "s3.amazonaws.com"
	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	sessionToken := os.Getenv("AWS_SESSION_TOKEN")

	if accessKeyID == "" || secretAccessKey == "" || sessionToken == "" {
		t.Skip("Skipping S3 tests if no creds provided")
	}

	ctx := testlogging.Context(t)

	cli, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, sessionToken),
		Secure: true,
		Region: "",
	})
	require.NoError(t, err)

	bucketName = fmt.Sprintf("engine-unit-tests-%s", randomString(4))
	err = cli.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	require.NoError(t, err)

	return bucketName, func() {
		objChan := make(chan minio.ObjectInfo)
		errCh := cli.RemoveObjects(ctx, bucketName, objChan, minio.RemoveObjectsOptions{})

		go func() {
			for removeErr := range errCh {
				t.Errorf("error removing key %s from bucket: %s", removeErr.ObjectName, removeErr.Err)
			}
		}()

		for obj := range cli.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
			Prefix:    "",
			Recursive: true,
		}) {
			objChan <- obj
		}

		close(objChan)

		retries := 10
		retryPeriod := 1 * time.Second

		var err error

		for range retries {
			time.Sleep(retryPeriod)

			err = cli.RemoveBucket(ctx, bucketName)
			if err == nil {
				break
			}
		}
		require.NoError(t, err)
	}
}

func TestWriteFilesBasicS3(t *testing.T) {
	bucketName, cleanupCB := makeTempS3Bucket(t)
	defer cleanupCB()

	t.Setenv(snapmeta.EngineModeEnvKey, snapmeta.EngineModeBasic)
	t.Setenv(snapmeta.S3BucketNameEnvKey, bucketName)

	ctx := testlogging.Context(t)

	th, eng, err := newTestHarness(ctx, t, s3DataRepoPath, s3MetadataRepoPath)
	if errors.Is(err, kopiarunner.ErrExeVariableNotSet) || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	require.NoError(t, err)

	defer func() {
		cleanupErr := th.Cleanup(ctx)
		require.NoError(t, cleanupErr)
	}()

	opts := map[string]string{}
	err = eng.Init(ctx)
	require.NoError(t, err)

	fileSize := int64(256 * 1024)
	numFiles := 10

	fioOpts := fio.Options{}.WithFileSize(fileSize).WithNumFiles(numFiles)
	fioRunner := th.FioRunner()
	err = fioRunner.WriteFiles("", fioOpts)
	require.NoError(t, err)

	snapIDs := eng.Checker.GetLiveSnapIDs()

	snapID, err := eng.Checker.TakeSnapshot(ctx, fioRunner.LocalDataDir, opts)
	require.NoError(t, err)

	err = eng.Checker.RestoreSnapshot(ctx, snapID, os.Stdout, opts)
	require.NoError(t, err)

	for _, sID := range snapIDs {
		err = eng.Checker.RestoreSnapshot(ctx, sID, os.Stdout, opts)
		require.NoError(t, err)
	}
}

func TestDeleteSnapshotS3(t *testing.T) {
	bucketName, cleanupCB := makeTempS3Bucket(t)
	defer cleanupCB()

	t.Setenv(snapmeta.EngineModeEnvKey, snapmeta.EngineModeBasic)
	t.Setenv(snapmeta.S3BucketNameEnvKey, bucketName)

	ctx := testlogging.Context(t)

	th, eng, err := newTestHarness(ctx, t, s3DataRepoPath, s3MetadataRepoPath)
	if errors.Is(err, kopiarunner.ErrExeVariableNotSet) || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	require.NoError(t, err)

	defer func() {
		cleanupErr := th.Cleanup(ctx)
		require.NoError(t, cleanupErr)
	}()

	opts := map[string]string{}
	err = eng.Init(ctx)
	require.NoError(t, err)

	fileSize := int64(256 * 1024)
	numFiles := 10

	fioOpts := fio.Options{}.WithFileSize(fileSize).WithNumFiles(numFiles)
	fioRunner := th.FioRunner()
	err = fioRunner.WriteFiles("", fioOpts)
	require.NoError(t, err)

	snapID, err := eng.Checker.TakeSnapshot(ctx, fioRunner.LocalDataDir, opts)
	require.NoError(t, err)

	err = eng.Checker.RestoreSnapshot(ctx, snapID, os.Stdout, opts)
	require.NoError(t, err)

	err = eng.Checker.DeleteSnapshot(ctx, snapID, opts)
	require.NoError(t, err)

	err = eng.Checker.RestoreSnapshot(ctx, snapID, os.Stdout, opts)
	if err == nil {
		t.Fatalf("Expected an error when trying to restore a deleted snapshot")
	}
}

func TestSnapshotVerificationFail(t *testing.T) {
	bucketName, cleanupCB := makeTempS3Bucket(t)
	defer cleanupCB()

	t.Setenv(snapmeta.EngineModeEnvKey, snapmeta.EngineModeBasic)
	t.Setenv(snapmeta.S3BucketNameEnvKey, bucketName)

	ctx := testlogging.Context(t)

	th, eng, err := newTestHarness(ctx, t, s3DataRepoPath, s3MetadataRepoPath)
	if errors.Is(err, kopiarunner.ErrExeVariableNotSet) || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	require.NoError(t, err)

	defer func() {
		cleanupErr := th.Cleanup(ctx)
		require.NoError(t, cleanupErr)
	}()

	opts := map[string]string{}
	err = eng.Init(ctx)
	require.NoError(t, err)

	// Perform writes
	fileSize := int64(256 * 1024)
	numFiles := 10
	fioOpt := fio.Options{}.WithFileSize(fileSize).WithNumFiles(numFiles)

	fioRunner := th.FioRunner()
	err = fioRunner.WriteFiles("", fioOpt)
	require.NoError(t, err)

	// Take a first snapshot
	snapID1, err := eng.Checker.TakeSnapshot(ctx, fioRunner.LocalDataDir, opts)
	require.NoError(t, err)

	// Get the metadata collected on that snapshot
	ssMeta1, err := eng.Checker.GetSnapshotMetadata(ctx, snapID1)
	require.NoError(t, err)

	// Do additional writes, writing 1 extra byte than before
	err = fioRunner.WriteFiles("", fioOpt.WithFileSize(fileSize+1))
	require.NoError(t, err)

	// Take a second snapshot
	snapID2, err := eng.Checker.TakeSnapshot(ctx, fioRunner.LocalDataDir, opts)
	require.NoError(t, err)

	// Get the second snapshot's metadata
	ssMeta2, err := eng.Checker.GetSnapshotMetadata(ctx, snapID2)
	require.NoError(t, err)

	// Swap second snapshot's validation data into the first's metadata
	ssMeta1.ValidationData = ssMeta2.ValidationData

	restoreDir, err := os.MkdirTemp(eng.Checker.RestoreDir, fmt.Sprintf("restore-snap-%v", snapID1))
	require.NoError(t, err)

	defer os.RemoveAll(restoreDir)

	// Restore snapshot ID 1 with snapshot 2's validation data in metadata, expect error
	err = eng.Checker.RestoreVerifySnapshot(ctx, snapID1, restoreDir, ssMeta1, os.Stdout, opts)
	if err == nil {
		t.Fatalf("Expected an integrity error when trying to restore a snapshot with incorrect metadata")
	}
}

func TestDataPersistency(t *testing.T) {
	t.Setenv(snapmeta.EngineModeEnvKey, snapmeta.EngineModeBasic)
	t.Setenv(snapmeta.S3BucketNameEnvKey, "")

	tempDir := testutil.TempDirectory(t)

	dataRepoPath := filepath.Join(tempDir, "data-repo-")
	metadataRepoPath := filepath.Join(tempDir, "metadata-repo-")

	ctx := testlogging.Context(t)

	th, eng, err := newTestHarness(ctx, t, dataRepoPath, metadataRepoPath)
	if errors.Is(err, kopiarunner.ErrExeVariableNotSet) || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	require.NoError(t, err)

	defer func() {
		cleanupErr := th.Cleanup(ctx)
		require.NoError(t, cleanupErr)
	}()

	opts := map[string]string{}
	err = eng.Init(ctx)
	require.NoError(t, err)

	// Perform writes
	fileSize := int64(256 * 1024)
	numFiles := 10

	fioOpt := fio.Options{}.WithFileSize(fileSize).WithNumFiles(numFiles)
	fioRunner := th.FioRunner()
	err = fioRunner.WriteFiles("", fioOpt)
	require.NoError(t, err)

	// Take a snapshot
	snapID, err := eng.Checker.TakeSnapshot(ctx, fioRunner.LocalDataDir, opts)
	require.NoError(t, err)

	// Get the walk data associated with the snapshot that was taken
	dataDirWalk, err := eng.Checker.GetSnapshotMetadata(ctx, snapID)
	require.NoError(t, err)

	// Save the snapshot ID index
	err = eng.saveSnapIDIndex(ctx)
	require.NoError(t, err)

	// Flush the snapshot metadata to persistent storage
	err = eng.MetaStore.FlushMetadata()
	require.NoError(t, err)

	// Create a new engine
	th2, eng2, err := newTestHarness(ctx, t, dataRepoPath, metadataRepoPath)
	require.NoError(t, err)

	defer func() {
		th2.eng.cleanComponents()
		th2.eng = nil
		th2.Cleanup(ctx)
	}()

	// Connect this engine to the same data and metadata repositories -
	// expect that the snapshot taken above will be found in metadata,
	// and the data will be chosen to be restored to this engine's DataDir
	// as a starting point.
	err = eng2.Init(ctx)
	require.NoError(t, err)

	fioRunner2 := th2.FioRunner()
	err = eng2.Checker.RestoreSnapshotToPath(ctx, snapID, fioRunner2.LocalDataDir, os.Stdout, opts)
	require.NoError(t, err)

	// Compare the data directory of the second engine with the fingerprint
	// of the snapshot taken earlier. They should match.
	err = fswalker.NewWalkCompare().Compare(ctx, fioRunner2.LocalDataDir, dataDirWalk.ValidationData, os.Stdout, opts)
	require.NoError(t, err)
}

func TestPickActionWeighted(t *testing.T) {
	for _, tc := range []struct {
		name             string
		inputCtrlWeights map[string]float64
		inputActionList  map[ActionKey]Action
	}{
		{
			name: "basic uniform",
			inputCtrlWeights: map[string]float64{
				"A": 1,
				"B": 1,
				"C": 1,
			},
			inputActionList: map[ActionKey]Action{
				"A": {},
				"B": {},
				"C": {},
			},
		},
		{
			name: "basic weighted",
			inputCtrlWeights: map[string]float64{
				"A": 1,
				"B": 10,
				"C": 100,
			},
			inputActionList: map[ActionKey]Action{
				"A": {},
				"B": {},
				"C": {},
			},
		},
		{
			name: "include a zero weight",
			inputCtrlWeights: map[string]float64{
				"A": 1,
				"B": 0,
				"C": 1,
			},
			inputActionList: map[ActionKey]Action{
				"A": {},
				"B": {},
				"C": {},
			},
		},
		{
			name: "include an ActionKey that is not in the action list",
			inputCtrlWeights: map[string]float64{
				"A": 1,
				"B": 1,
				"C": 1,
				"D": 100,
			},
			inputActionList: map[ActionKey]Action{
				"A": {},
				"B": {},
				"C": {},
			},
		},
	} {
		t.Log(tc.name)

		weightsSum := 0.0
		inputCtrlOpts := make(map[string]string)

		for k, v := range tc.inputCtrlWeights {
			// Do not weight actions that are not expected in the results
			if _, ok := tc.inputActionList[ActionKey(k)]; !ok {
				continue
			}

			inputCtrlOpts[k] = strconv.Itoa(int(v))
			weightsSum += v
		}

		numTestLoops := 100000

		results := make(map[ActionKey]int, len(tc.inputCtrlWeights))
		for range numTestLoops {
			results[pickActionWeighted(inputCtrlOpts, tc.inputActionList)]++
		}

		for actionKey, count := range results {
			p := tc.inputCtrlWeights[string(actionKey)] / weightsSum
			exp := p * float64(numTestLoops)

			errPcnt := math.Abs(exp-float64(count)) / exp
			if errPcnt > 0.1 {
				t.Errorf("Error in actual counts was above 10%% for %v (exp %v, actual %v)", actionKey, exp, count)
			}
		}
	}
}

func TestActionsFilesystem(t *testing.T) {
	t.Setenv(snapmeta.EngineModeEnvKey, snapmeta.EngineModeBasic)
	t.Setenv(snapmeta.S3BucketNameEnvKey, "")

	ctx := testlogging.Context(t)

	th, eng, err := newTestHarness(ctx, t, fsDataRepoPath, fsMetadataRepoPath)
	if errors.Is(err, kopiarunner.ErrExeVariableNotSet) || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	require.NoError(t, err)

	defer func() {
		cleanupErr := th.Cleanup(ctx)
		require.NoError(t, cleanupErr)

		os.RemoveAll(fsRepoBaseDirPath)
	}()

	err = eng.Init(ctx)
	require.NoError(t, err)

	actionOpts := ActionOpts{
		WriteRandomFilesActionKey: map[string]string{
			fiofilewriter.MaxDirDepthField:         "20",
			fiofilewriter.MaxFileSizeField:         strconv.Itoa(10 * 1024 * 1024),
			fiofilewriter.MinFileSizeField:         strconv.Itoa(10 * 1024 * 1024),
			fiofilewriter.MaxNumFilesPerWriteField: "10",
			fiofilewriter.MinNumFilesPerWriteField: "10",
			fiofilewriter.MaxDedupePercentField:    "100",
			fiofilewriter.MinDedupePercentField:    "100",
			fiofilewriter.DedupePercentStepField:   "1",
			fiofilewriter.IOLimitPerWriteAction:    "0",
		},
	}

	numActions := 10
	for range numActions {
		err := eng.RandomAction(ctx, actionOpts)
		if !(err == nil || errors.Is(err, robustness.ErrNoOp)) {
			t.Error("Hit error", err)
		}
	}
}

func TestActionsS3(t *testing.T) {
	bucketName, cleanupCB := makeTempS3Bucket(t)
	defer cleanupCB()

	t.Setenv(snapmeta.EngineModeEnvKey, snapmeta.EngineModeBasic)
	t.Setenv(snapmeta.S3BucketNameEnvKey, bucketName)

	ctx := testlogging.Context(t)

	th, eng, err := newTestHarness(ctx, t, s3DataRepoPath, s3MetadataRepoPath)
	if errors.Is(err, kopiarunner.ErrExeVariableNotSet) || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	require.NoError(t, err)

	defer func() {
		cleanupErr := th.Cleanup(ctx)
		require.NoError(t, cleanupErr)
	}()

	err = eng.Init(ctx)
	require.NoError(t, err)

	actionOpts := ActionOpts{
		WriteRandomFilesActionKey: map[string]string{
			fiofilewriter.MaxDirDepthField:         "20",
			fiofilewriter.MaxFileSizeField:         strconv.Itoa(10 * 1024 * 1024),
			fiofilewriter.MinFileSizeField:         strconv.Itoa(10 * 1024 * 1024),
			fiofilewriter.MaxNumFilesPerWriteField: "10",
			fiofilewriter.MinNumFilesPerWriteField: "10",
			fiofilewriter.MaxDedupePercentField:    "100",
			fiofilewriter.MinDedupePercentField:    "100",
			fiofilewriter.DedupePercentStepField:   "1",
			fiofilewriter.IOLimitPerWriteAction:    "0",
		},
	}

	numActions := 10
	for range numActions {
		err := eng.RandomAction(ctx, actionOpts)
		if !(err == nil || errors.Is(err, robustness.ErrNoOp)) {
			t.Error("Hit error", err)
		}
	}
}

func TestIOLimitPerWriteAction(t *testing.T) {
	// Instruct a write action to write a large amount of data, but add
	// an I/O limit parameter. Expect that the FIO action should limit
	// the amount of data it writes.
	t.Setenv(snapmeta.EngineModeEnvKey, snapmeta.EngineModeBasic)
	t.Setenv(snapmeta.S3BucketNameEnvKey, "")

	ctx := testlogging.Context(t)

	th, eng, err := newTestHarness(ctx, t, fsDataRepoPath, fsMetadataRepoPath)
	if errors.Is(err, kopiarunner.ErrExeVariableNotSet) || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	require.NoError(t, err)

	defer func() {
		cleanupErr := th.Cleanup(ctx)
		require.NoError(t, cleanupErr)

		os.RemoveAll(fsRepoBaseDirPath)
	}()

	err = eng.Init(ctx)
	require.NoError(t, err)

	ioLimitBytes := 1 * 1024 * 1024

	actionOpts := ActionOpts{
		ActionControlActionKey: map[string]string{
			string(SnapshotDirActionKey):              strconv.Itoa(0),
			string(RestoreSnapshotActionKey):          strconv.Itoa(0),
			string(DeleteRandomSnapshotActionKey):     strconv.Itoa(0),
			string(WriteRandomFilesActionKey):         strconv.Itoa(1),
			string(DeleteRandomSubdirectoryActionKey): strconv.Itoa(0),
		},
		WriteRandomFilesActionKey: map[string]string{
			fiofilewriter.MaxDirDepthField:         "2",
			fiofilewriter.MaxFileSizeField:         strconv.Itoa(10 * 1024 * 1024),
			fiofilewriter.MinFileSizeField:         strconv.Itoa(10 * 1024 * 1024),
			fiofilewriter.MaxNumFilesPerWriteField: "100",
			fiofilewriter.MinNumFilesPerWriteField: "100",
			fiofilewriter.IOLimitPerWriteAction:    strconv.Itoa(ioLimitBytes),
		},
	}

	err = eng.RandomAction(ctx, actionOpts)
	require.NoError(t, err)

	size := 0

	// The FIO write operation is expected to create multiple files.
	// The behavior is that I/O will begin on a file, writing randomly
	// to that file until the I/O limit is hit. Thus we expect to see
	// one file with non-zero size (should be approx. between min-max file size
	// parameters, i.e. 10 MiB) and 1 MiB or less of non-zero data
	// written to it, due to the I/O limit.
	walkFunc := func(path string, info fs.FileInfo, err error) error {
		if !info.IsDir() && info.Size() > 0 {
			fileContentB, err := os.ReadFile(path)
			require.NoError(t, err)

			nonZeroByteCount := 0

			for _, byteVal := range fileContentB {
				if byteVal > 0 {
					nonZeroByteCount++
				}
			}

			size += nonZeroByteCount
		}

		return nil
	}

	fioPath := eng.FileWriter.DataDirectory(ctx)

	// Walk the FIO data directory tree, counting the non-zero data written.
	err = filepath.Walk(fioPath, walkFunc)
	require.NoError(t, err)

	if got, want := size, ioLimitBytes; got > want {
		t.Fatalf("IO write limit exceeded for write action. Wrote %v B with io limit %v", got, want)
	}

	// We might expect that a '0' gets written as part of the FIO data. This
	// means the count of non-zero bytes above might be a bit less than the exact
	// i/o limit parameter. We shouldn't expect a large percent of '0' though, so
	// this check will ensure fio didn't write significantly less than the limit.
	// A fraction of at least 95% non-zero should be very conservative.
	const thresholdNonZeroFraction = 0.95

	if got, want := float64(size), float64(ioLimitBytes)*thresholdNonZeroFraction; got <= want {
		t.Fatalf("IO write limit exceeded for write action. Wrote %v B with io limit %v", got, want)
	}
}

func TestStatsPersist(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "stats-persist-test")
	require.NoError(t, err)

	defer os.RemoveAll(tmpDir)

	snapStore, err := snapmeta.NewPersister(tmpDir)
	if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
		t.Skip(err)
	}

	require.NoError(t, err)

	err = snapStore.ConnectOrCreateFilesystem(tmpDir)
	require.NoError(t, err)

	actionstats := &ActionStats{
		Count:        120,
		TotalRuntime: 25 * time.Hour,
		MinRuntime:   5 * time.Minute,
		MaxRuntime:   35 * time.Minute,
	}

	creationTime := clock.Now().Add(-time.Hour)

	eng := &Engine{
		MetaStore: snapStore,
		CumulativeStats: Stats{
			ActionCounter: 11235,
			CreationTime:  creationTime,
			PerActionStats: map[ActionKey]*ActionStats{
				ActionKey("some-action"): actionstats,
			},
			DataRestoreCount: 99,
		},
	}

	err = eng.saveStats(ctx)
	require.NoError(t, err)

	err = eng.MetaStore.FlushMetadata()
	require.NoError(t, err)

	snapStoreNew, err := snapmeta.NewPersister(tmpDir)
	require.NoError(t, err)

	// Connect to the same metadata store
	err = snapStoreNew.ConnectOrCreateFilesystem(tmpDir)
	require.NoError(t, err)

	err = snapStoreNew.LoadMetadata()
	require.NoError(t, err)

	engNew := &Engine{
		MetaStore: snapStoreNew,
	}

	err = engNew.loadStats(ctx)
	require.NoError(t, err)

	if got, want := engNew.Stats(), eng.Stats(); got != want {
		t.Errorf("Stats do not match\n%v\n%v", got, want)
	}

	fmt.Println(eng.Stats())
	fmt.Println(engNew.Stats())
}

func TestLogsPersist(t *testing.T) {
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "logs-persist-test")
	require.NoError(t, err)

	defer os.RemoveAll(tmpDir)

	snapStore, err := snapmeta.NewPersister(tmpDir)
	if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
		t.Skip(err)
	}

	require.NoError(t, err)

	err = snapStore.ConnectOrCreateFilesystem(tmpDir)
	require.NoError(t, err)

	logData := Log{
		Log: []*LogEntry{
			{
				StartTime: clock.Now().Add(-time.Hour),
				EndTime:   clock.Now(),
				Action:    ActionKey("some action"),
				Error:     "some error",
				Idx:       11235,
				ActionOpts: map[string]string{
					"opt1": "opt1 value",
				},
				CmdOpts: map[string]string{
					"cmdOpt": "cmdOptVal",
				},
			},
		},
	}

	eng := &Engine{
		MetaStore: snapStore,
		EngineLog: logData,
	}

	err = eng.saveLog(ctx)
	require.NoError(t, err)

	err = eng.MetaStore.FlushMetadata()
	require.NoError(t, err)

	snapStoreNew, err := snapmeta.NewPersister(tmpDir)
	require.NoError(t, err)

	// Connect to the same metadata store
	err = snapStoreNew.ConnectOrCreateFilesystem(tmpDir)
	require.NoError(t, err)

	err = snapStoreNew.LoadMetadata()
	require.NoError(t, err)

	engNew := &Engine{
		MetaStore: snapStoreNew,
	}

	err = engNew.loadLog(ctx)
	require.NoError(t, err)

	if got, want := engNew.EngineLog.String(), eng.EngineLog.String(); got != want {
		t.Errorf("Logs do not match\n%v\n%v", got, want)
	}
}

type testHarness struct {
	fw *fiofilewriter.FileWriter
	ks *snapmeta.KopiaSnapshotter
	kp *snapmeta.KopiaPersister

	baseDir string

	eng *Engine
}

func newTestHarness(ctx context.Context, t *testing.T, dataRepoPath, metaRepoPath string) (*testHarness, *Engine, error) {
	t.Helper()

	var (
		th  = &testHarness{}
		err error
	)

	if th.baseDir, err = os.MkdirTemp("", "engine-data-"); err != nil {
		return nil, nil, err
	}

	if th.fw, err = fiofilewriter.New(); err != nil {
		th.Cleanup(ctx)
		return nil, nil, err
	}

	if th.ks, err = snapmeta.NewSnapshotter(th.baseDir); err != nil {
		th.Cleanup(ctx)
		return nil, nil, err
	}

	if err = th.ks.ConnectOrCreateRepo(dataRepoPath); err != nil {
		th.Cleanup(ctx)
		return nil, nil, err
	}

	if th.kp, err = snapmeta.NewPersister(th.baseDir); err != nil {
		th.Cleanup(ctx)
		return nil, nil, err
	}

	if err = th.kp.ConnectOrCreateRepo(metaRepoPath); err != nil {
		th.Cleanup(ctx)
		return nil, nil, err
	}

	if th.eng, err = New(th.args()); err != nil {
		th.Cleanup(ctx)
		return nil, nil, err
	}

	return th, th.eng, err
}

func (th *testHarness) args() *Args {
	return &Args{
		MetaStore:  th.kp,
		TestRepo:   th.ks,
		FileWriter: th.fw,
		WorkingDir: th.baseDir,
	}
}

func (th *testHarness) FioRunner() *fio.Runner {
	return th.fw.Runner
}

func (th *testHarness) Cleanup(ctx context.Context) error {
	var err error

	if th.eng != nil {
		err = th.eng.Shutdown(ctx)
	}

	if th.fw != nil {
		th.fw.Cleanup()
	}

	if th.ks != nil {
		if sc := th.ks.ServerCmd(); sc != nil {
			if err = sc.Process.Signal(syscall.SIGTERM); err != nil {
				log.Println("Failed to send termination signal to kopia server process:", err)
			}
		}

		th.ks.Cleanup()
	}

	if th.kp != nil {
		th.kp.Cleanup()
	}

	os.RemoveAll(th.baseDir)

	return err
}
