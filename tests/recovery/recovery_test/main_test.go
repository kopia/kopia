//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package recovery

import (
	"flag"
	"log"
	"os"
	"path"
	"testing"
)

const (
	dataSubPath = "recovery-data"
	dirPath     = "kopia_dummy_repo"
	dataPath    = "crash-consistency-data"
)

var repoPathPrefix = flag.String("repo-path-prefix", "", "Point the robustness tests at this path prefix")

func TestMain(m *testing.M) {
	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)

	th := &kopiaRecoveryTestHarness{}
	th.init(dataRepoPath)

	// run the tests
	result := m.Run()

	os.Exit(result)
}

type kopiaRecoveryTestHarness struct {
	dataRepoPath string
}

func (th *kopiaRecoveryTestHarness) init(dataRepoPath string) {
	th.dataRepoPath = dataRepoPath

	kopiaExe := os.Getenv("KOPIA_EXE")
	if kopiaExe == "" {
		log.Println("Skipping recovery tests because KOPIA_EXE is not set")
		os.Exit(0)
	}
}
