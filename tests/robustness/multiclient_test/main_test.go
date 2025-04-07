//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package multiclienttest

import (
	"context"
	"flag"
	"log"
	"os"
	"testing"

	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/robustness/multiclient_test/framework"
	"github.com/kopia/kopia/tests/robustness/multiclient_test/storagestats"
)

// Variables for use in the test functions.
var (
	eng *engine.Engine
	th  *framework.TestHarness
)

func TestMain(m *testing.M) {
	flag.Parse()

	// A high-level client is required for harness initialization and cleanup steps.
	ctx := framework.NewClientContext(context.Background())

	th = framework.NewHarness(ctx)

	eng = th.Engine()

	// Perform setup needed to get storage stats.
	dirs := th.GetDirsToLog(ctx)
	log.Printf("Logging storage stats for %v", dirs)
	err := storagestats.LogStorageStats(ctx, dirs)
	if err != nil {
		log.Printf("Error collecting the logs: %s", err.Error())
	}

	// run the tests
	result := m.Run()

	// Log storage stats after the test run.
	err = storagestats.LogStorageStats(ctx, dirs)
	if err != nil {
		log.Printf("Error collecting the logs: %s", err.Error())
	}

	err = th.Cleanup(ctx)
	if err != nil {
		log.Printf("Error cleaning up the engine: %s\n", err.Error())
		os.Exit(2)
	}

	os.Exit(result)
}
