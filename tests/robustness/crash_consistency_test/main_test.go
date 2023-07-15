//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package consistency

import (
	"flag"
	"log"
	"os"
	"path"
	"testing"

	"github.com/kopia/kopia/tests/robustness/snapmeta"
)

const (
	dirPath  = "kopia_dummy_repo"
	dataPath = "crash-consistency-data"
)

var repoPathPrefix = flag.String("repo-path-prefix", "", "Point the robustness tests at this path prefix")

// var bm *blobmanipulator.BlobManipulator = nil

func TestMain(m *testing.M) {
	dataRepoPath := path.Join(*repoPathPrefix, dirPath, dataPath)

	th := &kopiaTestHarness{}
	th.init(dataRepoPath)

	// run the tests
	result := m.Run()

	os.Exit(result)
}

type kopiaTestHarness struct {
	dataRepoPath string
	baseDirPath  string

	snapshotter *snapmeta.KopiaSnapshotter
}

func (th *kopiaTestHarness) init(dataRepoPath string) {
	th.dataRepoPath = dataRepoPath

	kopiaExe := os.Getenv("KOPIA_EXE")
	if kopiaExe == "" {
		log.Println("Skipping recovery tests because KOPIA_EXE is not set")
		os.Exit(0)
	}
}
