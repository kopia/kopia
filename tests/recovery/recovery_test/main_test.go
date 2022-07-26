//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package recovery

import (
	"flag"
	"os"
	"path"
	"testing"
)

const (
	dataSubPath = "recovery-data"
)

var repoPathPrefix = flag.String("repo-path-prefix", "/Users/chaitali.gondhalekar/Work/Kasten/kopia_dummy_repo/", "Point the robustness tests at this path prefix")

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
}
