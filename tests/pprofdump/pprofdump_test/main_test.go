//go:build darwin || linux
// +build darwin linux

package pprofdump_test

import (
	"flag"
	"log"
	"os"
	"path"
	"testing"
)

const (
	dataSubPath = "pprofdump-data"
	dirPath     = "kopia_dummy_repo"
	dataPath    = "crash-consistency-data"
)

var repoPathPrefix = flag.String("repo-path-prefix", "", "Point the robustness tests at this path prefix")

func TestMain(m *testing.M) {
	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)

	th := &kopiaPprofDumpRepositoryConnectTest{}
	th.init(dataRepoPath)

	// run the tests
	result := m.Run()

	os.Exit(result)
}

type kopiaPprofDumpRepositoryConnectTest struct {
	dataRepoPath string
}

func (th *kopiaPprofDumpRepositoryConnectTest) init(dataRepoPath string) {
	th.dataRepoPath = dataRepoPath

	kopiaExe := os.Getenv("KOPIA_EXE")
	if kopiaExe == "" {
		log.Println("Skipping pprofdump tests because KOPIA_EXE is not set")
		os.Exit(0)
	}
}
