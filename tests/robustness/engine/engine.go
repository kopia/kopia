//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

// Package engine provides the framework for a snapshot repository testing engine
package engine

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/robustness/checker"
)

var (
	// ErrInvalidArgs is returned if the constructor arguments are incorrect.
	ErrInvalidArgs = fmt.Errorf("invalid arguments")

	noSpaceOnDeviceMatchStr = "no space left on device"
)

// Args contain the parameters for the engine constructor.
type Args struct {
	// Interfaces used by the engine.
	MetaStore  robustness.Persister
	TestRepo   robustness.Snapshotter
	FileWriter robustness.FileWriter

	// WorkingDir is a directory to use for temporary data.
	WorkingDir string

	// SyncRepositories should be set to true to reconcile differences.
	SyncRepositories bool
}

// Validate checks the arguments for correctness.
func (a *Args) Validate() error {
	if a.MetaStore == nil || a.TestRepo == nil || a.FileWriter == nil || a.WorkingDir == "" {
		return ErrInvalidArgs
	}

	return nil
}

// New creates an Engine.
func New(args *Args) (*Engine, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}

	var (
		e = &Engine{
			MetaStore:   args.MetaStore,
			TestRepo:    args.TestRepo,
			FileWriter:  args.FileWriter,
			baseDirPath: args.WorkingDir,
			RunStats: Stats{
				RunCounter:     1,
				CreationTime:   clock.Now(),
				PerActionStats: make(map[ActionKey]*ActionStats),
			},
		}
		err error
	)

	if err = e.setupLogging(); err != nil {
		e.cleanComponents()
		return nil, err
	}

	e.Checker, err = checker.NewChecker(e.TestRepo, e.MetaStore, e.baseDirPath)
	if err != nil {
		e.cleanComponents()
		return nil, err
	}

	e.Checker.RecoveryMode = args.SyncRepositories
	e.cleanupRoutines = append(e.cleanupRoutines, e.Checker.Cleanup)

	// // Create a kopia runner in order to run delete commands directly
	// e.KopiaCommandRunner, err = kopiarunner.NewKopiaSnapshotter(e.baseDirPath)
	// if err != nil {
	// 	e.cleanComponents()
	// 	return nil, err
	// }

	return e, nil
}

// Engine is the outer level testing framework for robustness testing.
type Engine struct {
	FileWriter robustness.FileWriter
	TestRepo   robustness.Snapshotter
	MetaStore  robustness.Persister

	Checker         *checker.Checker
	cleanupRoutines []func()
	baseDirPath     string

	RunStats        Stats
	CumulativeStats Stats
	statsMux        sync.RWMutex

	EngineLog Log
	logMux    sync.RWMutex
}

// Shutdown makes a last snapshot then flushes the metadata and prints the final statistics.
func (e *Engine) Shutdown(ctx context.Context) error {
	// Perform a snapshot action to capture the state of the data directory
	// at the end of the run
	lastWriteEntry := e.EngineLog.FindLastThisRun(WriteRandomFilesActionKey)
	lastSnapEntry := e.EngineLog.FindLastThisRun(SnapshotDirActionKey)

	if lastWriteEntry != nil {
		if lastSnapEntry == nil || lastSnapEntry.Idx < lastWriteEntry.Idx {
			// Only force a final snapshot if the data tree has been modified since the last snapshot
			e.ExecAction(ctx, SnapshotDirActionKey, make(map[string]string)) //nolint:errcheck
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

	e.RunStats.RunTime = clock.Now().Sub(e.RunStats.CreationTime)
	e.CumulativeStats.RunTime += e.RunStats.RunTime

	defer e.cleanComponents()

	if e.MetaStore != nil {
		err := e.saveLog(ctx)
		if err != nil {
			return err
		}

		err = e.saveStats(ctx)
		if err != nil {
			return err
		}

		err = e.saveSnapIDIndex(ctx)
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

// cleanComponents cleans up each component part of the test engine.
func (e *Engine) cleanComponents() {
	for _, f := range e.cleanupRoutines {
		if f != nil {
			f()
		}
	}
}

// Init initializes the Engine and performs a consistency check.
func (e *Engine) Init(ctx context.Context) error {
	err := e.MetaStore.LoadMetadata()
	if err != nil {
		return err
	}

	err = e.loadStats(ctx)
	if err != nil {
		return err
	}

	e.CumulativeStats.RunCounter++

	err = e.loadLog(ctx)
	if err != nil {
		return err
	}

	err = e.loadSnapIDIndex(ctx)
	if err != nil {
		return err
	}

	return e.Checker.VerifySnapshotMetadata(ctx)
}
