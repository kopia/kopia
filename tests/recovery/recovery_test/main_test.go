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
	dataSubPath     = "recovery-data"
	metadataSubPath = "recovery-metadata"
)

var repoPathPrefix = flag.String("repo-path-prefix", "", "Point the robustness tests at this path prefix")

func TestMain(m *testing.M) {
	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)
	metadataRepoPath := path.Join(*repoPathPrefix, metadataSubPath)

	th := &kopiaRecoveryTestHarness{}
	th.init(dataRepoPath, metadataRepoPath)

	// run the tests
	result := m.Run()

	os.Exit(result)
}

type kopiaRecoveryTestHarness struct {
	dataRepoPath string
	metaRepoPath string
}

func (th *kopiaRecoveryTestHarness) init(dataRepoPath, metaRepoPath string) {
	th.dataRepoPath = dataRepoPath
	th.metaRepoPath = metaRepoPath
}
