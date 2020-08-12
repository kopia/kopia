// +build darwin,amd64 linux,amd64

// Package engine provides the framework for a snapshot repository testing engine
package engine

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"

	"github.com/kopia/kopia/tests/robustness/checker"
	"github.com/kopia/kopia/tests/robustness/snap"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/fswalker"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

const (
	// S3BucketNameEnvKey is the environment variable required to connect to a repo on S3
	S3BucketNameEnvKey = "S3_BUCKET_NAME"
)

// ErrS3BucketNameEnvUnset is the error returned when the S3BucketNameEnvKey environment variable is not set
var ErrS3BucketNameEnvUnset = fmt.Errorf("environment variable required: %v", S3BucketNameEnvKey)

// Engine is the outer level testing framework for robustness testing
type Engine struct {
	FileWriter      *fio.Runner
	TestRepo        snap.Snapshotter
	MetaStore       snapmeta.Persister
	Checker         *checker.Checker
	cleanupRoutines []func()
}

// NewEngine instantiates a new Engine and returns its pointer. It is
// currently created with:
// - FIO file writer
// - Kopia test repo snapshotter
// - Kopia metadata storage repo
// - FSWalker data integrity checker
func NewEngine() (*Engine, error) {
	e := new(Engine)

	var err error

	// Fill the file writer
	e.FileWriter, err = fio.NewRunner()
	if err != nil {
		e.Cleanup() //nolint:errcheck
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, e.FileWriter.Cleanup)

	// Fill Snapshotter interface
	kopiaSnapper, err := kopiarunner.NewKopiaSnapshotter()
	if err != nil {
		e.Cleanup() //nolint:errcheck
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, kopiaSnapper.Cleanup)
	e.TestRepo = kopiaSnapper

	// Fill the snapshot store interface
	snapStore, err := snapmeta.New()
	if err != nil {
		e.Cleanup() //nolint:errcheck
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, snapStore.Cleanup)

	e.MetaStore = snapStore

	// Create the data integrity checker
	chk, err := checker.NewChecker(kopiaSnapper, snapStore, fswalker.NewWalkCompare())
	e.cleanupRoutines = append(e.cleanupRoutines, chk.Cleanup)

	if err != nil {
		e.Cleanup() //nolint:errcheck
		return nil, err
	}

	e.Checker = chk

	return e, nil
}

// Cleanup cleans up after each component of the test engine
func (e *Engine) Cleanup() error {
	defer e.cleanup()

	if e.MetaStore != nil {
		return e.MetaStore.FlushMetadata()
	}

	return nil
}

func (e *Engine) cleanup() {
	for _, f := range e.cleanupRoutines {
		f()
	}
}

// InitS3 attempts to connect to a test repo and metadata repo on S3. If connection
// is successful, the engine is populated with the metadata associated with the
// snapshot in that repo. A new repo will be created if one does not already
// exist.
func (e *Engine) InitS3(ctx context.Context, testRepoPath, metaRepoPath string) error {
	bucketName := os.Getenv(S3BucketNameEnvKey)
	if bucketName == "" {
		return ErrS3BucketNameEnvUnset
	}

	err := e.MetaStore.ConnectOrCreateS3(bucketName, metaRepoPath)
	if err != nil {
		return err
	}

	err = e.MetaStore.LoadMetadata()
	if err != nil {
		return err
	}

	err = e.TestRepo.ConnectOrCreateS3(bucketName, testRepoPath)
	if err != nil {
		return err
	}

	_, _, err = e.TestRepo.Run("policy", "set", "--global", "--keep-latest", strconv.Itoa(1<<31-1))
	if err != nil {
		return err
	}

	err = e.Checker.VerifySnapshotMetadata()
	if err != nil {
		return err
	}

	snapIDs := e.Checker.GetLiveSnapIDs()
	if len(snapIDs) > 0 {
		randSnapID := snapIDs[rand.Intn(len(snapIDs))]

		err = e.Checker.RestoreSnapshotToPath(ctx, randSnapID, e.FileWriter.LocalDataDir, os.Stdout)
		if err != nil {
			return err
		}
	}

	return nil
}

// InitFilesystem attempts to connect to a test repo and metadata repo on the local
// filesystem. If connection is successful, the engine is populated with the
// metadata associated with the snapshot in that repo. A new repo will be created if
// one does not already exist.
func (e *Engine) InitFilesystem(ctx context.Context, testRepoPath, metaRepoPath string) error {
	err := e.MetaStore.ConnectOrCreateFilesystem(metaRepoPath)
	if err != nil {
		return err
	}

	err = e.MetaStore.LoadMetadata()
	if err != nil {
		return err
	}

	err = e.TestRepo.ConnectOrCreateFilesystem(testRepoPath)
	if err != nil {
		return err
	}

	err = e.Checker.VerifySnapshotMetadata()
	if err != nil {
		return err
	}

	snapIDs := e.Checker.GetSnapIDs()
	if len(snapIDs) > 0 {
		randSnapID := snapIDs[rand.Intn(len(snapIDs))]

		err = e.Checker.RestoreSnapshotToPath(ctx, randSnapID, e.FileWriter.LocalDataDir, os.Stdout)
		if err != nil {
			return err
		}
	}

	return nil
}
