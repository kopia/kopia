//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package recovery_test

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
	"github.com/stretchr/testify/require"
)

// var bm *BlobManipulator

// const (
// 	dataSubPath     = "robustness-data"
// 	metadataSubPath = "robustness-metadata"
// 	defaultTestDur  = 5 * time.Minute
// )

// var (
// 	repoPathPrefix = flag.String("repo-path-prefix", "/Users/chaitali.gondhalekar/Work/Kasten/kopia_dummy_repo/", "Point the robustness tests at this path prefix")
// )

func TestSnapshotFix(t *testing.T) {

	// assumption: the test is run on filesystem and not directly on S3

	fmt.Printf("Inside the test")

	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)
	// metadataRepoPath := path.Join(*repoPathPrefix, metadataSubPath)

	// current state: a test repo is available in /test-repo/robustness-data
	// test main connects to test-repo on filesystem, test -repo = SUT
	// restores a snapshot from test-repo into /tmp/

	// create a base dir
	baseDir := makeBaseDir()

	bm, err := NewBlobManipulator(baseDir)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			log.Println("Skipping robustness tests because KOPIA_EXE is not set")
		} else {
			log.Println("Error creating Blob Manipulator:", err)
		}
	}

	// connect to or create date repo path
	bm.DirCreater = getSnapshotter(baseDir)
	bm.ConnectOrCreateRepo(dataRepoPath)
	bm.dataRepoPath = dataRepoPath

	// populate the kopia repo under test with random snapshots
	bm.SetUpSystemUnderTest()

	// delete random blob
	// assumption: the repo contains "p" blobs to delete, else the test is a no-op
	bm.DeleteBlob("")

	// try to restore the latest snapshot
	// this should error out
	// how to create a temporary dir to restore a snapshot?
	restoreDir, err := os.MkdirTemp("", "restore-data-")
	if err != nil {
		log.Println("Error creating temp dir:", err)
	}

	// try to restore a snapshot, this should error out
	stdout, err := bm.RestoreGivenOrRandomSnapshot("", restoreDir)
	require.Error(t, err)

	// extract out object ID needed to be used in snapshot fix command
	blobID := getBlobIDToBeDeleted(stdout)
	stdout, err = bm.SnapshotFixRemoveFilesByBlobID(blobID)
	if err != nil {
		log.Println("Error repairing the kopia repository:", stdout, err)
	}

	// filename := getFilenameToBeDeleted(stdout)
	// stdout, err = bm.SnapshotFixRemoveFilesByFilename(filename)
	// if err != nil {
	// 	log.Println("Error repairing the kopia repository:", stdout, err)
	// }

	// restore a random snapshot
	_, err = bm.RestoreGivenOrRandomSnapshot("", restoreDir)
	require.NoError(t, err)

}

func makeBaseDir() (baseDir string) {
	baseDir, err := os.MkdirTemp("", "base-dir-")
	if err != nil {
		log.Println("Error creating temp dir:", err)
		return ""
	}

	return baseDir
}

func getBlobIDToBeDeleted(stdout string) string {
	s1 := strings.Split(stdout, ":")
	wanted_index := -1
	for i, s := range s1 {
		if strings.Contains(s, "unable to open object") {
			wanted_index = i + 1
			break
		}
	}
	if wanted_index == -1 {
		return ""
	}
	return s1[wanted_index]

}

func getFilenameToBeDeleted(stdout string) string {
	s1 := strings.Split(stdout, ":")
	wanted_index := -1
	for i, s := range s1 {
		if strings.Contains(s, "unable to open snapshot file for") {
			wanted_index = i
			break
		}
	}
	if wanted_index == -1 {
		return ""
	}
	s2 := strings.Split(s1[wanted_index], " ")
	return s2[len(s2)-1]

}

func getSnapshotter(baseDirPath string) (ks *snapmeta.KopiaSnapshotter) {
	ks, err := snapmeta.NewSnapshotter(baseDirPath)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			log.Println("Skipping robustness tests because KOPIA_EXE is not set")

		} else {
			log.Println("Error creating kopia Snapshotter:", err)
		}

		return nil
	}

	return ks
}
