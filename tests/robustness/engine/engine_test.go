// +build darwin,amd64 linux,amd64

// Package engine provides the framework for a snapshot repository testing engine
package engine

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/fswalker"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

var (
	fsMetadataRepoPath = filepath.Join("/tmp", "metadata-repo")
	s3MetadataRepoPath = filepath.Join("some/path", "metadata-repo")
	fsDataRepoPath     = filepath.Join("/tmp", "data-repo")
	s3DataRepoPath     = filepath.Join("some/path", "data-repo")
)

func TestEngineWritefilesBasicFS(t *testing.T) {
	eng, err := NewEngine()
	if err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)
	}()

	ctx := context.TODO()
	err = eng.InitFilesystem(ctx, fsDataRepoPath, fsMetadataRepoPath)
	testenv.AssertNoError(t, err)

	fileSize := int64(256 * 1024 * 1024)
	numFiles := 10

	fioOpt := fio.Options{}.WithFileSize(fileSize).WithNumFiles(numFiles)

	err = eng.FileWriter.WriteFiles("", fioOpt)
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

func TestWriteFilesBasicS3(t *testing.T) {
	eng, err := NewEngine()
	if err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)
	}()

	ctx := context.TODO()
	err = eng.InitS3(ctx, s3DataRepoPath, s3MetadataRepoPath)
	testenv.AssertNoError(t, err)

	fileSize := int64(256 * 1024 * 1024)
	numFiles := 10

	fioOpt := fio.Options{}.WithFileSize(fileSize).WithNumFiles(numFiles)

	err = eng.FileWriter.WriteFiles("", fioOpt)
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
	eng, err := NewEngine()
	if err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)
	}()

	ctx := context.TODO()
	err = eng.InitS3(ctx, s3DataRepoPath, s3MetadataRepoPath)
	testenv.AssertNoError(t, err)

	fileSize := int64(256 * 1024 * 1024)
	numFiles := 10

	fioOpt := fio.Options{}.WithFileSize(fileSize).WithNumFiles(numFiles)

	err = eng.FileWriter.WriteFiles("", fioOpt)
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
	eng, err := NewEngine()
	if err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet) {
		t.Skip(err)
	}

	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)
	}()

	ctx := context.TODO()
	err = eng.InitS3(ctx, s3DataRepoPath, s3MetadataRepoPath)
	testenv.AssertNoError(t, err)

	// Perform writes
	fileSize := int64(256 * 1024 * 1024)
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

	defer os.RemoveAll(restoreDir) //nolint:errcheck

	// Restore snapshot ID 1 with snapshot 2's validation data in metadata, expect error
	err = eng.Checker.RestoreVerifySnapshot(ctx, snapID1, restoreDir, ssMeta1, os.Stdout)
	if err == nil {
		t.Fatalf("Expected an integrity error when trying to restore a snapshot with incorrect metadata")
	}
}

func TestDataPersistency(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	testenv.AssertNoError(t, err)

	defer os.RemoveAll(tempDir) //nolint:errcheck

	eng, err := NewEngine()
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
	fileSize := int64(256 * 1024 * 1024)
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
	eng2, err := NewEngine()
	testenv.AssertNoError(t, err)

	defer eng2.cleanup()

	// Connect this engine to the same data and metadata repositories -
	// expect that the snapshot taken above will be found in metadata,
	// and the data will be chosen to be restored to this engine's DataDir
	// as a starting point.
	err = eng2.InitFilesystem(ctx, dataRepoPath, metadataRepoPath)
	testenv.AssertNoError(t, err)

	// Compare the data directory of the second engine with the fingerprint
	// of the snapshot taken earlier. They should match.
	err = fswalker.NewWalkCompare().Compare(ctx, eng2.FileWriter.LocalDataDir, dataDirWalk.ValidationData, os.Stdout)
	testenv.AssertNoError(t, err)
}
