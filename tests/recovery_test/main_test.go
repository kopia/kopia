//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package recovery_test

import (
	"flag"
	"fmt"
	"os"
	"path"
	"testing"
	"time"
)

const (
	dataSubPath     = "robustness-data"
	metadataSubPath = "robustness-metadata"
	defaultTestDur  = 5 * time.Minute
)

var (
	repoPathPrefix = flag.String("repo-path-prefix", "/Users/chaitali.gondhalekar/Work/Kasten/kopia_dummy_repo/", "Point the robustness tests at this path prefix")
)

func TestMain(m *testing.M) {

	fmt.Printf("Inside the test")

	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)
	metadataRepoPath := path.Join(*repoPathPrefix, metadataSubPath)

	th := &kopiaRecoveryTestHarness{}
	th.init(dataRepoPath, metadataRepoPath)

	// Restore a random snapshot into the data directory

	// run the tests
	result := m.Run()

	os.Exit(result)
}

type kopiaRecoveryTestHarness struct {
	dataRepoPath string
	metaRepoPath string

	skipTest bool
}

func (th *kopiaRecoveryTestHarness) init(dataRepoPath, metaRepoPath string) {
	th.dataRepoPath = dataRepoPath
	th.metaRepoPath = metaRepoPath
}
