// +build darwin,amd64 linux,amd64

package robustness

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

var eng *engine.Engine

const (
	dataSubPath     = "robustness-data"
	metadataSubPath = "robustness-metadata"
	defaultTestDur  = 5 * time.Minute
)

var (
	randomizedTestDur = flag.Duration("rand-test-duration", defaultTestDur, "Set the duration for the randomized test")
	repoPathPrefix    = flag.String("repo-path-prefix", "", "Point the robustness tests at this path prefix")
)

func TestMain(m *testing.M) {
	flag.Parse()

	var err error

	eng, err = engine.NewEngine("")

	switch {
	case err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet):
		fmt.Println("Skipping robustness tests if KOPIA_EXE is not set")
		os.Exit(0)
	case err != nil:
		log.Fatalln("error on engine creation:", err)
	}

	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)
	metadataRepoPath := path.Join(*repoPathPrefix, metadataSubPath)

	// Try to reconcile metadata if it is out of sync with the repo state
	eng.Checker.RecoveryMode = true

	// Initialize the engine, connecting it to the repositories
	err = eng.Init(context.Background(), dataRepoPath, metadataRepoPath)
	if err != nil {
		// Clean the temporary dirs from the file system, don't write out the
		// metadata, in case there was an issue loading it
		eng.CleanComponents()
		log.Fatalln("error initializing engine for S3:", err)
	}

	// Restore a random snapshot into the data directory
	_, err = eng.ExecAction(engine.RestoreIntoDataDirectoryActionKey, nil)
	if err != nil && err != engine.ErrNoOp {
		eng.Cleanup()
		log.Fatalln("error restoring into the data directory:", err)
	}

	result := m.Run()

	err = eng.Cleanup()
	if err != nil {
		log.Printf("error cleaning up the engine: %s\n", err.Error())
		os.Exit(2)
	}

	os.Exit(result)
}
