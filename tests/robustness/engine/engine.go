// +build darwin,amd64 linux,amd64

// Package engine provides the framework for a snapshot repository testing engine
package engine

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kopia/kopia/tests/robustness/checker"
	"github.com/kopia/kopia/tests/robustness/snap"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/fswalker"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

const (
	// S3BucketNameEnvKey is the environment variable required to connect to a repo on S3.
	S3BucketNameEnvKey = "S3_BUCKET_NAME"
)

var (
	// ErrNoOp is thrown when an action could not do anything useful.
	ErrNoOp = fmt.Errorf("no-op")
	// ErrCannotPerformIO is returned if the engine determines there is not enough space
	// to write files.
	ErrCannotPerformIO = fmt.Errorf("cannot perform i/o")
	// ErrS3BucketNameEnvUnset is the error returned when the S3BucketNameEnvKey environment variable is not set.
	ErrS3BucketNameEnvUnset = fmt.Errorf("environment variable required: %v", S3BucketNameEnvKey)
	noSpaceOnDeviceMatchStr = "no space left on device"
)

// Engine is the outer level testing framework for robustness testing.
type Engine struct {
	FileWriter      *fio.Runner
	TestRepo        snap.Snapshotter
	MetaStore       snapmeta.Persister
	Checker         *checker.Checker
	cleanupRoutines []func()
	baseDirPath     string

	RunStats        Stats
	CumulativeStats Stats
	EngineLog       Log
}

// NewEngine instantiates a new Engine and returns its pointer. It is
// currently created with:
// - FIO file writer
// - Kopia test repo snapshotter
// - Kopia metadata storage repo
// - FSWalker data integrity checker.
func NewEngine(workingDir string) (*Engine, error) {
	baseDirPath, err := ioutil.TempDir(workingDir, "engine-data-")
	if err != nil {
		return nil, err
	}

	e := &Engine{
		baseDirPath: baseDirPath,
		RunStats: Stats{
			RunCounter:     1,
			CreationTime:   time.Now(),
			PerActionStats: make(map[ActionKey]*ActionStats),
		},
	}

	// Fill the file writer
	e.FileWriter, err = fio.NewRunner()
	if err != nil {
		e.CleanComponents()
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, e.FileWriter.Cleanup)

	// Fill Snapshotter interface
	kopiaSnapper, err := kopiarunner.NewKopiaSnapshotter(baseDirPath)
	if err != nil {
		e.CleanComponents()
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, kopiaSnapper.Cleanup)
	e.TestRepo = kopiaSnapper

	// Fill the snapshot store interface
	snapStore, err := snapmeta.New(baseDirPath)
	if err != nil {
		e.CleanComponents()
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, snapStore.Cleanup)

	e.MetaStore = snapStore

	err = e.setupLogging()
	if err != nil {
		e.CleanComponents()
		return nil, err
	}

	// Create the data integrity checker
	chk, err := checker.NewChecker(kopiaSnapper, snapStore, fswalker.NewWalkCompare(), baseDirPath)
	e.cleanupRoutines = append(e.cleanupRoutines, chk.Cleanup)

	if err != nil {
		e.CleanComponents()
		return nil, err
	}

	e.Checker = chk

	return e, nil
}

// Cleanup cleans up after each component of the test engine.
func (e *Engine) Cleanup() error {
	// Perform a snapshot action to capture the state of the data directory
	// at the end of the run
	lastWriteEntry := e.EngineLog.FindLastThisRun(WriteRandomFilesActionKey)
	lastSnapEntry := e.EngineLog.FindLastThisRun(SnapshotRootDirActionKey)

	if lastWriteEntry != nil {
		if lastSnapEntry == nil || lastSnapEntry.Idx < lastWriteEntry.Idx {
			// Only force a final snapshot if the data tree has been modified since the last snapshot
			e.ExecAction(SnapshotRootDirActionKey, make(map[string]string)) //nolint:errcheck
		}
	}

	cleanupSummaryBuilder := new(strings.Builder)
	cleanupSummaryBuilder.WriteString("\n================\n")
	cleanupSummaryBuilder.WriteString("Cleanup Summary:\n\n")
	cleanupSummaryBuilder.WriteString(e.Stats())
	cleanupSummaryBuilder.WriteString("\n\n")
	cleanupSummaryBuilder.WriteString(e.EngineLog.StringThisRun())
	cleanupSummaryBuilder.WriteString("\n")

	log.Print(cleanupSummaryBuilder.String())

	e.RunStats.RunTime = time.Since(e.RunStats.CreationTime)
	e.CumulativeStats.RunTime += e.RunStats.RunTime

	defer e.CleanComponents()

	if e.MetaStore != nil {
		err := e.SaveLog()
		if err != nil {
			return err
		}

		err = e.SaveStats()
		if err != nil {
			return err
		}

		return e.MetaStore.FlushMetadata()
	}

	return nil
}

func (e *Engine) setupLogging() error {
	dirPath := e.MetaStore.GetPersistDir()

	newLogPath := filepath.Join(dirPath, e.formatLogName())

	f, err := os.Create(newLogPath)
	if err != nil {
		return err
	}

	// Write to both stderr and persistent log file
	wrt := io.MultiWriter(os.Stderr, f)
	log.SetOutput(wrt)

	return nil
}

func (e *Engine) formatLogName() string {
	st := e.RunStats.CreationTime
	return fmt.Sprintf("Log_%s", st.Format("2006_01_02_15_04_05"))
}

// CleanComponents cleans up each component part of the test engine.
func (e *Engine) CleanComponents() {
	for _, f := range e.cleanupRoutines {
		if f != nil {
			f()
		}
	}

	os.RemoveAll(e.baseDirPath) //nolint:errcheck
}

// Init initializes the Engine to a repository location according to the environment setup.
// - If S3_BUCKET_NAME is set, initialize S3
// - Else initialize filesystem.
func (e *Engine) Init(ctx context.Context, testRepoPath, metaRepoPath string) error {
	switch {
	case os.Getenv(S3BucketNameEnvKey) != "":
		bucketName := os.Getenv(S3BucketNameEnvKey)
		return e.InitS3(ctx, bucketName, testRepoPath, metaRepoPath)
	default:
		return e.InitFilesystem(ctx, testRepoPath, metaRepoPath)
	}
}

// InitS3 attempts to connect to a test repo and metadata repo on S3. If connection
// is successful, the engine is populated with the metadata associated with the
// snapshot in that repo. A new repo will be created if one does not already
// exist.
func (e *Engine) InitS3(ctx context.Context, bucketName, testRepoPath, metaRepoPath string) error {
	err := e.MetaStore.ConnectOrCreateS3(bucketName, metaRepoPath)
	if err != nil {
		return err
	}

	err = e.TestRepo.ConnectOrCreateS3(bucketName, testRepoPath)
	if err != nil {
		return err
	}

	return e.init(ctx)
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

	err = e.TestRepo.ConnectOrCreateFilesystem(testRepoPath)
	if err != nil {
		return err
	}

	return e.init(ctx)
}

func (e *Engine) init(ctx context.Context) error {
	err := e.MetaStore.LoadMetadata()
	if err != nil {
		return err
	}

	err = e.LoadStats()
	if err != nil {
		return err
	}

	e.CumulativeStats.RunCounter++

	err = e.LoadLog()
	if err != nil {
		return err
	}

	_, _, err = e.TestRepo.Run("policy", "set", "--global", "--keep-latest", strconv.Itoa(1<<31-1), "--compression", "s2-default")
	if err != nil {
		return err
	}

	return e.Checker.VerifySnapshotMetadata()
}
