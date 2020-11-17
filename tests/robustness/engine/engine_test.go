// +build darwin,amd64 linux,amd64

// Package engine provides the framework for a snapshot repository testing engine
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/minio/minio-go/v6"
	"github.com/minio/minio-go/v6/pkg/credentials"

	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/testenv"
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
	eng, err := NewEngine("")
	if err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)

		os.RemoveAll(fsRepoBaseDirPath)
	}()

	ctx := context.TODO()
	err = eng.InitFilesystem(ctx, fsDataRepoPath, fsMetadataRepoPath)
	testenv.AssertNoError(t, err)

	fileSize := int64(256 * 1024)
	numFiles := 10

	fioOpts := fio.Options{}.WithFileSize(fileSize).WithNumFiles(numFiles)

	err = eng.FileWriter.WriteFiles("", fioOpts)
	testenv.AssertNoError(t, err)

	snapIDs := eng.Checker.GetSnapIDs()

	snapID, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.LocalDataDir)
	testenv.AssertNoError(t, err)

	err = eng.Checker.RestoreSnapshot(ctx, snapID, os.Stdout)
	testenv.AssertNoError(t, err)

	for _, sID := range snapIDs {
		err = eng.Checker.RestoreSnapshot(ctx, sID, os.Stdout)
		testenv.AssertNoError(t, err)
	}
}

func randomString(n int) string {
	b := make([]byte, n)
	io.ReadFull(rand.Reader, b)

	return hex.EncodeToString(b)
}

func makeTempS3Bucket(t *testing.T) (bucketName string, cleanupCB func()) {
	endpoint := "s3.amazonaws.com"
	accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	sessionToken := os.Getenv("AWS_SESSION_TOKEN")

	if accessKeyID == "" || secretAccessKey == "" || sessionToken == "" {
		t.Skip("Skipping S3 tests if no creds provided")
	}

	secure := true
	region := ""
	cli, err := minio.NewWithCredentials(endpoint, credentials.NewStaticV4(accessKeyID, secretAccessKey, sessionToken), secure, region)
	testenv.AssertNoError(t, err)

	bucketName = fmt.Sprintf("engine-unit-tests-%s", randomString(4))
	err = cli.MakeBucket(bucketName, "")
	testenv.AssertNoError(t, err)

	return bucketName, func() {
		objNameCh := make(chan string)
		errCh := cli.RemoveObjects(bucketName, objNameCh)

		go func() {
			for removeErr := range errCh {
				t.Errorf("error removing key %s from bucket: %s", removeErr.ObjectName, removeErr.Err)
			}
		}()

		recursive := true
		doneCh := make(chan struct{})

		defer close(doneCh)

		for obj := range cli.ListObjects(bucketName, "", recursive, doneCh) {
			objNameCh <- obj.Key
		}

		close(objNameCh)

		retries := 10
		retryPeriod := 1 * time.Second

		var err error

		for retry := 0; retry < retries; retry++ {
			time.Sleep(retryPeriod)

			err = cli.RemoveBucket(bucketName)
			if err == nil {
				break
			}
		}
		testenv.AssertNoError(t, err)
	}
}

func TestWriteFilesBasicS3(t *testing.T) {
	bucketName, cleanupCB := makeTempS3Bucket(t)
	defer cleanupCB()

	eng, err := NewEngine("")
	if err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)
	}()

	ctx := context.TODO()
	err = eng.InitS3(ctx, bucketName, s3DataRepoPath, s3MetadataRepoPath)
	testenv.AssertNoError(t, err)

	fileSize := int64(256 * 1024)
	numFiles := 10

	fioOpts := fio.Options{}.WithFileSize(fileSize).WithNumFiles(numFiles)

	err = eng.FileWriter.WriteFiles("", fioOpts)
	testenv.AssertNoError(t, err)

	snapIDs := eng.Checker.GetLiveSnapIDs()

	snapID, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.LocalDataDir)
	testenv.AssertNoError(t, err)

	err = eng.Checker.RestoreSnapshot(ctx, snapID, os.Stdout)
	testenv.AssertNoError(t, err)

	for _, sID := range snapIDs {
		err = eng.Checker.RestoreSnapshot(ctx, sID, os.Stdout)
		testenv.AssertNoError(t, err)
	}
}

func TestDeleteSnapshotS3(t *testing.T) {
	bucketName, cleanupCB := makeTempS3Bucket(t)
	defer cleanupCB()

	eng, err := NewEngine("")
	if err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)
	}()

	ctx := context.TODO()
	err = eng.InitS3(ctx, bucketName, s3DataRepoPath, s3MetadataRepoPath)
	testenv.AssertNoError(t, err)

	fileSize := int64(256 * 1024)
	numFiles := 10

	fioOpts := fio.Options{}.WithFileSize(fileSize).WithNumFiles(numFiles)

	err = eng.FileWriter.WriteFiles("", fioOpts)
	testenv.AssertNoError(t, err)

	snapID, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.LocalDataDir)
	testenv.AssertNoError(t, err)

	err = eng.Checker.RestoreSnapshot(ctx, snapID, os.Stdout)
	testenv.AssertNoError(t, err)

	err = eng.Checker.DeleteSnapshot(ctx, snapID)
	testenv.AssertNoError(t, err)

	err = eng.Checker.RestoreSnapshot(ctx, snapID, os.Stdout)
	if err == nil {
		t.Fatalf("Expected an error when trying to restore a deleted snapshot")
	}
}

func TestSnapshotVerificationFail(t *testing.T) {
	bucketName, cleanupCB := makeTempS3Bucket(t)
	defer cleanupCB()

	eng, err := NewEngine("")
	if err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)
	}()

	ctx := context.TODO()
	err = eng.InitS3(ctx, bucketName, s3DataRepoPath, s3MetadataRepoPath)
	testenv.AssertNoError(t, err)

	// Perform writes
	fileSize := int64(256 * 1024)
	numFiles := 10
	fioOpt := fio.Options{}.WithFileSize(fileSize).WithNumFiles(numFiles)

	err = eng.FileWriter.WriteFiles("", fioOpt)
	testenv.AssertNoError(t, err)

	// Take a first snapshot
	snapID1, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.LocalDataDir)
	testenv.AssertNoError(t, err)

	// Get the metadata collected on that snapshot
	ssMeta1, err := eng.Checker.GetSnapshotMetadata(snapID1)
	testenv.AssertNoError(t, err)

	// Do additional writes, writing 1 extra byte than before
	err = eng.FileWriter.WriteFiles("", fioOpt.WithFileSize(fileSize+1))
	testenv.AssertNoError(t, err)

	// Take a second snapshot
	snapID2, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.LocalDataDir)
	testenv.AssertNoError(t, err)

	// Get the second snapshot's metadata
	ssMeta2, err := eng.Checker.GetSnapshotMetadata(snapID2)
	testenv.AssertNoError(t, err)

	// Swap second snapshot's validation data into the first's metadata
	ssMeta1.ValidationData = ssMeta2.ValidationData

	restoreDir, err := ioutil.TempDir(eng.Checker.RestoreDir, fmt.Sprintf("restore-snap-%v", snapID1))
	testenv.AssertNoError(t, err)

	defer os.RemoveAll(restoreDir)

	// Restore snapshot ID 1 with snapshot 2's validation data in metadata, expect error
	err = eng.Checker.RestoreVerifySnapshot(ctx, snapID1, restoreDir, ssMeta1, os.Stdout)
	if err == nil {
		t.Fatalf("Expected an integrity error when trying to restore a snapshot with incorrect metadata")
	}
}

func TestDataPersistency(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	testenv.AssertNoError(t, err)

	defer os.RemoveAll(tempDir)

	eng, err := NewEngine("")
	if err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)
	}()

	dataRepoPath := filepath.Join(tempDir, "data-repo-")
	metadataRepoPath := filepath.Join(tempDir, "metadata-repo-")

	ctx := context.TODO()
	err = eng.InitFilesystem(ctx, dataRepoPath, metadataRepoPath)
	testenv.AssertNoError(t, err)

	// Perform writes
	fileSize := int64(256 * 1024)
	numFiles := 10

	fioOpt := fio.Options{}.WithFileSize(fileSize).WithNumFiles(numFiles)

	err = eng.FileWriter.WriteFiles("", fioOpt)
	testenv.AssertNoError(t, err)

	// Take a snapshot
	snapID, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.LocalDataDir)
	testenv.AssertNoError(t, err)

	// Get the walk data associated with the snapshot that was taken
	dataDirWalk, err := eng.Checker.GetSnapshotMetadata(snapID)
	testenv.AssertNoError(t, err)

	// Flush the snapshot metadata to persistent storage
	err = eng.MetaStore.FlushMetadata()
	testenv.AssertNoError(t, err)

	// Create a new engine
	eng2, err := NewEngine("")
	testenv.AssertNoError(t, err)

	defer eng2.CleanComponents()

	// Connect this engine to the same data and metadata repositories -
	// expect that the snapshot taken above will be found in metadata,
	// and the data will be chosen to be restored to this engine's DataDir
	// as a starting point.
	err = eng2.InitFilesystem(ctx, dataRepoPath, metadataRepoPath)
	testenv.AssertNoError(t, err)

	err = eng2.Checker.RestoreSnapshotToPath(ctx, snapID, eng2.FileWriter.LocalDataDir, os.Stdout)
	testenv.AssertNoError(t, err)

	// Compare the data directory of the second engine with the fingerprint
	// of the snapshot taken earlier. They should match.
	err = fswalker.NewWalkCompare().Compare(ctx, eng2.FileWriter.LocalDataDir, dataDirWalk.ValidationData, os.Stdout)
	testenv.AssertNoError(t, err)
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
		for loop := 0; loop < numTestLoops; loop++ {
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
	eng, err := NewEngine("")
	if err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)

		os.RemoveAll(fsRepoBaseDirPath)
	}()

	ctx := context.TODO()
	err = eng.InitFilesystem(ctx, fsDataRepoPath, fsMetadataRepoPath)
	testenv.AssertNoError(t, err)

	actionOpts := ActionOpts{
		WriteRandomFilesActionKey: map[string]string{
			MaxDirDepthField:         "20",
			MaxFileSizeField:         strconv.Itoa(10 * 1024 * 1024),
			MinFileSizeField:         strconv.Itoa(10 * 1024 * 1024),
			MaxNumFilesPerWriteField: "10",
			MinNumFilesPerWriteField: "10",
			MaxDedupePercentField:    "100",
			MinDedupePercentField:    "100",
			DedupePercentStepField:   "1",
			IOLimitPerWriteAction:    "0",
		},
	}

	numActions := 10
	for loop := 0; loop < numActions; loop++ {
		err := eng.RandomAction(actionOpts)
		if !(err == nil || err == ErrNoOp) {
			t.Error("Hit error", err)
		}
	}
}

func TestActionsS3(t *testing.T) {
	bucketName, cleanupCB := makeTempS3Bucket(t)
	defer cleanupCB()

	eng, err := NewEngine("")
	if err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)
	}()

	ctx := context.TODO()
	err = eng.InitS3(ctx, bucketName, s3DataRepoPath, s3MetadataRepoPath)
	testenv.AssertNoError(t, err)

	actionOpts := ActionOpts{
		WriteRandomFilesActionKey: map[string]string{
			MaxDirDepthField:         "20",
			MaxFileSizeField:         strconv.Itoa(10 * 1024 * 1024),
			MinFileSizeField:         strconv.Itoa(10 * 1024 * 1024),
			MaxNumFilesPerWriteField: "10",
			MinNumFilesPerWriteField: "10",
			MaxDedupePercentField:    "100",
			MinDedupePercentField:    "100",
			DedupePercentStepField:   "1",
			IOLimitPerWriteAction:    "0",
		},
	}

	numActions := 10
	for loop := 0; loop < numActions; loop++ {
		err := eng.RandomAction(actionOpts)
		if !(err == nil || err == ErrNoOp) {
			t.Error("Hit error", err)
		}
	}
}

func TestIOLimitPerWriteAction(t *testing.T) {
	// Instruct a write action to write an enormous amount of data
	// that should take longer than this timeout without "io_limit",
	// but finish in less time with "io_limit". Command instructs fio
	// to generate 100 files x 10 MB each = 1 GB of i/o. The limit is
	// set to 1 MB.
	const timeout = 10 * time.Second

	eng, err := NewEngine("")
	if err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)

		os.RemoveAll(fsRepoBaseDirPath)
	}()

	ctx := context.TODO()
	err = eng.InitFilesystem(ctx, fsDataRepoPath, fsMetadataRepoPath)
	testenv.AssertNoError(t, err)

	actionOpts := ActionOpts{
		ActionControlActionKey: map[string]string{
			string(SnapshotRootDirActionKey):          strconv.Itoa(0),
			string(RestoreSnapshotActionKey):          strconv.Itoa(0),
			string(DeleteRandomSnapshotActionKey):     strconv.Itoa(0),
			string(WriteRandomFilesActionKey):         strconv.Itoa(1),
			string(DeleteRandomSubdirectoryActionKey): strconv.Itoa(0),
		},
		WriteRandomFilesActionKey: map[string]string{
			MaxDirDepthField:         "2",
			MaxFileSizeField:         strconv.Itoa(10 * 1024 * 1024),
			MinFileSizeField:         strconv.Itoa(10 * 1024 * 1024),
			MaxNumFilesPerWriteField: "100",
			MinNumFilesPerWriteField: "100",
			IOLimitPerWriteAction:    strconv.Itoa(1 * 1024 * 1024),
		},
	}

	st := time.Now()

	numActions := 1
	for loop := 0; loop < numActions; loop++ {
		err := eng.RandomAction(actionOpts)
		testenv.AssertNoError(t, err)
	}

	if time.Since(st) > timeout {
		t.Errorf("IO limit parameter did not cut down on the fio runtime")
	}
}

func TestStatsPersist(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "stats-persist-test")
	testenv.AssertNoError(t, err)

	defer os.RemoveAll(tmpDir)

	snapStore, err := snapmeta.New(tmpDir)
	if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
		t.Skip(err)
	}

	testenv.AssertNoError(t, err)

	err = snapStore.ConnectOrCreateFilesystem(tmpDir)
	testenv.AssertNoError(t, err)

	actionstats := &ActionStats{
		Count:        120,
		TotalRuntime: 25 * time.Hour,
		MinRuntime:   5 * time.Minute,
		MaxRuntime:   35 * time.Minute,
	}

	creationTime := time.Now().Add(-time.Hour)

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

	err = eng.SaveStats()
	testenv.AssertNoError(t, err)

	err = eng.MetaStore.FlushMetadata()
	testenv.AssertNoError(t, err)

	snapStoreNew, err := snapmeta.New(tmpDir)
	testenv.AssertNoError(t, err)

	// Connect to the same metadata store
	err = snapStoreNew.ConnectOrCreateFilesystem(tmpDir)
	testenv.AssertNoError(t, err)

	err = snapStoreNew.LoadMetadata()
	testenv.AssertNoError(t, err)

	engNew := &Engine{
		MetaStore: snapStoreNew,
	}

	err = engNew.LoadStats()
	testenv.AssertNoError(t, err)

	if got, want := engNew.Stats(), eng.Stats(); got != want {
		t.Errorf("Stats do not match\n%v\n%v", got, want)
	}

	fmt.Println(eng.Stats())
	fmt.Println(engNew.Stats())
}

func TestLogsPersist(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "logs-persist-test")
	testenv.AssertNoError(t, err)

	defer os.RemoveAll(tmpDir)

	snapStore, err := snapmeta.New(tmpDir)
	if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
		t.Skip(err)
	}

	testenv.AssertNoError(t, err)

	err = snapStore.ConnectOrCreateFilesystem(tmpDir)
	testenv.AssertNoError(t, err)

	log := Log{
		Log: []*LogEntry{
			{
				StartTime: time.Now().Add(-time.Hour),
				EndTime:   time.Now(),
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
		EngineLog: log,
	}

	err = eng.SaveLog()
	testenv.AssertNoError(t, err)

	err = eng.MetaStore.FlushMetadata()
	testenv.AssertNoError(t, err)

	snapStoreNew, err := snapmeta.New(tmpDir)
	testenv.AssertNoError(t, err)

	// Connect to the same metadata store
	err = snapStoreNew.ConnectOrCreateFilesystem(tmpDir)
	testenv.AssertNoError(t, err)

	err = snapStoreNew.LoadMetadata()
	testenv.AssertNoError(t, err)

	engNew := &Engine{
		MetaStore: snapStoreNew,
	}

	err = engNew.LoadLog()
	testenv.AssertNoError(t, err)

	if got, want := engNew.EngineLog.String(), eng.EngineLog.String(); got != want {
		t.Errorf("Logs do not match\n%v\n%v", got, want)
	}
}
