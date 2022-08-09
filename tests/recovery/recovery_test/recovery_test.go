//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package recovery

import (
	"errors"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/tests/recovery/blobmanipulator"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

func TestSnapshotFix(t *testing.T) {
	// assumption: the test is run on filesystem & not directly on object store
	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)

	baseDir := t.TempDir()
	if baseDir == "" {
		t.FailNow()
	}

	bm, err := blobmanipulator.NewBlobManipulator(baseDir, dataRepoPath)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			t.Skip("Skipping recovery tests because KOPIA_EXE is not set")
		} else {
			t.Skip("Error creating Blob Manipulator:", err)
		}
	}

	bm.DataRepoPath = dataRepoPath

	// populate the kopia repo under test with random snapshots
	bm.CanRunMaintenance = false

	err = bm.SetUpSystemUnderTest()
	if err != nil {
		t.FailNow()
	}

	kopiaExe := os.Getenv("KOPIA_EXE")
	cmd := exec.Command(kopiaExe, "maintenance", "run", "--full", "--force", "--safety", "none")

	err = cmd.Start()
	if err != nil {
		t.FailNow()
	}

	// kill the kopia command before it exits
	time.AfterFunc(10*time.Millisecond, func() {
		cmd.Process.Kill()
	})

	// delete random blob
	// assumption: the repo contains "p" blobs to delete, else the test will fail
	err = bm.DeleteBlob("")
	if err != nil {
		log.Println("Error deleting kopia blob: ", err)
		t.FailNow()
	}

	// Create a temporary dir to restore a snapshot
	restoreDir := t.TempDir()
	if restoreDir == "" {
		t.FailNow()
	}

	_, err = bm.RunMaintenance()
	require.NoError(t, err)

	// try to restore a snapshot, this should error out
	stdout, err := bm.RestoreGivenOrRandomSnapshot("", restoreDir)
	require.Error(t, err)

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
	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath, "-fix-invalid")

	baseDir := t.TempDir()
	if baseDir == "" {
		t.FailNow()
	}

	bm, err := blobmanipulator.NewBlobManipulator(baseDir, dataRepoPath)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			log.Println("Skipping recovery tests because KOPIA_EXE is not set")
		} else {
			log.Println("Error creating Blob Manipulator:", err)
		}

		t.FailNow()
	}

	bm.DataRepoPath = dataRepoPath

	// populate the kopia repo under test with random snapshots
	bm.CanRunMaintenance = false

	err = bm.SetUpSystemUnderTest()
	if err != nil {
		t.FailNow()
	}

	kopiaExe := os.Getenv("KOPIA_EXE")
	cmd := exec.Command(kopiaExe, "maintenance", "run", "--full", "--force", "--safety", "none")

	err = cmd.Start()
	if err != nil {
		t.FailNow()
	}

	// kill the kopia command before it exits
	time.AfterFunc(10*time.Millisecond, func() {
		cmd.Process.Kill()
	})

	// delete random blob
	// assumption: the repo contains "p" blobs to delete, else the test will fail
	err = bm.DeleteBlob("")
	if err != nil {
		log.Println("Error deleting kopia blob: ", err)
		t.FailNow()
	}

	// Create a temporary dir to restore a snapshot
	restoreDir := t.TempDir()
	if restoreDir == "" {
		t.FailNow()
	}

	// try to restore a snapshot, this should error out
	_, err = bm.RestoreGivenOrRandomSnapshot("", restoreDir)
	require.Error(t, err)

	// fix all the invalid files
	stdout, err := bm.SnapshotFixInvalidFiles("--verify-files-percent=100")
	if err != nil {
		log.Println("Error repairing the kopia repository:", stdout, err)
		t.FailNow()
	}

	// restore a random snapshot
	_, err = bm.RestoreGivenOrRandomSnapshot("", restoreDir)
	require.NoError(t, err)
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
