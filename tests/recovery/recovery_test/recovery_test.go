//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package recovery

import (
	"errors"
	"log"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/tests/recovery/blobmanipulator"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

func TestSnapshotFix(t *testing.T) {
	// assumption: the test is run on filesystem & not directly on object store
	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)

	baseDir := makeDir("base-dir-")
	if baseDir == "" {
		t.FailNow()
	}

	ks := getSnapshotter(baseDir, dataRepoPath)
	if ks == nil {
		t.FailNow()
	}

	bm, err := blobmanipulator.NewBlobManipulator(baseDir)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			t.Skip("Skipping recovery tests because KOPIA_EXE is not set")
		} else {
			t.Skip("Error creating Blob Manipulator:", err)
		}
	}

	bm.DirCreater = ks

	// err = bm.ConnectOrCreateRepo(dataRepoPath)
	// if err != nil {
	// 	t.FailNow()
	// }

	bm.DataRepoPath = dataRepoPath

	// populate the kopia repo under test with random snapshots
	bm.CanRunMaintenance = false
	err = bm.SetUpSystemUnderTest()
	if err != nil {
		t.FailNow()
	}

	// delete random blob
	// assumption: the repo contains "p" blobs to delete, else the test will fail
	err = bm.DeleteBlob("")
	if err != nil {
		log.Println("Error deleting kopia blob: ", err)
		t.FailNow()
	}

	snapshot, _, err := bm.TakeSnapshot(bm.PathToTakeSnapshot)
	require.NoError(t, err)

	_, err = bm.DeleteSnapshot(snapshot)
	require.NoError(t, err)

	// Create a temporary dir to restore a snapshot
	restoreDir := makeDir("restore-data-")
	if restoreDir == "" {
		t.FailNow()
	}

	// try to restore a snapshot, this should error out
	stdout, err := bm.RestoreGivenOrRandomSnapshot("", restoreDir)
	require.Error(t, err)

	// stdout, err := bm.RunMaintenance()

	// snapshot, _, err = bm.TakeSnapshot(bm.PathToTakeSnapshot)
	// require.NoError(t, err)

	// stdout, err := bm.RunMaintenance()
	// require.Error(t, err)

	// extract out object ID needed to be used in snapshot fix command
	blobID := getBlobIDToBeDeleted(stdout)

	stdout, err = bm.SnapshotFixRemoveFilesByBlobID(blobID)
	if err != nil {
		log.Println("Error repairing the kopia repository:", stdout, err)
		t.FailNow()
	}

	// restore a random snapshot
	_, err = bm.RestoreGivenOrRandomSnapshot("", restoreDir)
	require.NoError(t, err)
}

func TestSnapshotFixInvalidFiles(t *testing.T) {
	// assumption: the test is run on filesystem & not directly on object store
	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)

	baseDir := makeDir("base-dir-")
	if baseDir == "" {
		t.FailNow()
	}

	bm, err := blobmanipulator.NewBlobManipulator(baseDir)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			log.Println("Skipping recovery tests because KOPIA_EXE is not set")
		} else {
			log.Println("Error creating Blob Manipulator:", err)
		}

		t.FailNow()
	}

	bm.DirCreater = getSnapshotter(baseDir, dataRepoPath)
	if bm.DirCreater == nil {
		t.FailNow()
	}

	err = bm.ConnectOrCreateRepo(dataRepoPath)
	if err != nil {
		t.FailNow()
	}

	bm.DataRepoPath = dataRepoPath

	// populate the kopia repo under test with random snapshots
	bm.CanRunMaintenance = false
	err = bm.SetUpSystemUnderTest()
	if err != nil {
		t.FailNow()
	}

	// delete random blob
	// assumption: the repo contains "p" blobs to delete, else the test will fail
	err = bm.DeleteBlob("")
	if err != nil {
		log.Println("Error deleting kopia blob: ", err)
		t.FailNow()
	}

	// Create a temporary dir to restore a snapshot
	restoreDir := makeDir("restore-data-")
	if restoreDir == "" {
		t.FailNow()
	}

	// try to restore a snapshot, this should error out
	stdout, err := bm.RestoreGivenOrRandomSnapshot("", restoreDir)
	require.Error(t, err)

	// stdout, err := bm.RunMaintenance()
	// require.Error(t, err)

	// fix all the invalid files
	stdout, err = bm.SnapshotFixInvalidFiles("--verify-files-percent=100")
	if err != nil {
		log.Println("Error repairing the kopia repository:", stdout, err)
		t.FailNow()
	}

	// restore a random snapshot
	_, err = bm.RestoreGivenOrRandomSnapshot("", restoreDir)
	require.NoError(t, err)
}

func makeDir(dirName string) string {
	baseDir, err := os.MkdirTemp("", dirName)
	if err != nil {
		log.Println("Error creating temp dir:", err)
		return ""
	}

	return baseDir
}

func getBlobIDToBeDeleted(stdout string) string {
	s1 := strings.Split(stdout, ":")

	wantedIndex := -1

	for i, s := range s1 {
		if strings.Contains(s, "unable to open object") {
			wantedIndex = i + 1
			break
		}
	}

	if wantedIndex == -1 {
		return ""
	}

	return strings.TrimSpace(s1[wantedIndex])
}

func getSnapshotter(baseDirPath string, dataRepoPath string) *snapmeta.KopiaSnapshotter {
	ks, err := snapmeta.NewSnapshotter(baseDirPath)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			log.Println("Skipping recovery tests because KOPIA_EXE is not set")
		} else {
			log.Println("Error creating kopia Snapshotter:", err)
		}

		return nil
	}

	if err = ks.ConnectOrCreateRepo(dataRepoPath); err != nil {
		log.Println("Error initializing kopia Snapshotter:", err)
		return nil
	}

	return ks
}
