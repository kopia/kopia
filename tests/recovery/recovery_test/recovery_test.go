//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package recovery

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/fs/localfs"
	"github.com/kopia/kopia/internal/diff"
	"github.com/kopia/kopia/tests/recovery/blobmanipulator"
	"github.com/kopia/kopia/tests/testenv"
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

// TestConsistencyWhenKill9AfterModify will test the data consistency while it encountered kill -9 signal.
func TestConsistencyWhenKill9AfterModify(t *testing.T) {
	// assumption: the test is run on filesystem & not directly on object store
	dataRepoPath := path.Join(*repoPathPrefix, dirPath, dataPath)

	baseDir := t.TempDir()
	require.NotEmpty(t, baseDir, "TempDir() did not generate a valid dir")

	bm, err := blobmanipulator.NewBlobManipulator(baseDir, dataRepoPath)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			t.Skip("Skipping crash consistency tests because KOPIA_EXE is not set")
		}

		t.Skip("Error creating SnapshotTester:", err)
	}

	bm.DataRepoPath = dataRepoPath

	// create a snapshot for initialized data
	_, err = bm.SetUpSystemWithOneSnapshot()
	require.NoError(t, err)

	cmpDir := bm.PathToTakeSnapshot

	// add files
	fileSize := 1 * 1024 * 1024
	numFiles := 200

	err = bm.GenerateRandomFiles(fileSize, numFiles)
	require.NoError(t, err)

	newDir := bm.PathToTakeSnapshot

	// connect with repository with the environment configuration, otherwise it will display "ERROR open repository: repository is not connected.kopia connect repo".
	kopiaExe := os.Getenv("KOPIA_EXE")

	cmd := exec.Command(kopiaExe, "repo", "connect", "filesystem", "--path="+dataRepoPath, "--content-cache-size-mb", "500", "--metadata-cache-size-mb", "500", "--no-check-for-updates")
	env := []string{"KOPIA_PASSWORD=" + testenv.TestRepoPassword}
	cmd.Env = append(os.Environ(), env...)

	o, err := cmd.CombinedOutput()
	require.NoError(t, err)
	t.Log(string(o))

	// create snapshot with StderrPipe
	cmd = exec.Command(kopiaExe, "snap", "create", newDir, "--json", "--parallel=1")

	// kill the kopia command before it exits
	t.Logf("Kill the kopia command before it exits:")
	killOnCondition(t, cmd)

	t.Logf("Verify snapshot corruption:")
	// verify snapshot corruption
	err = bm.VerifySnapshot()
	require.NoError(t, err)

	// Create a temporary dir to restore a snapshot
	restoreDir := t.TempDir()
	require.NotEmpty(t, restoreDir, "TempDir() did not generate a valid dir")

	// try to restore a snapshot without any error messages.
	stdout, err := bm.RestoreGivenOrRandomSnapshot("", restoreDir)
	require.NoError(t, err)

	t.Log(stdout)
	t.Log("Compare restored data and original data:")
	CompareDirs(t, restoreDir, cmpDir)
}

func killOnCondition(t *testing.T, cmd *exec.Cmd) {
	t.Helper()

	stderrPipe, err := cmd.StderrPipe()
	require.NoError(t, err)

	// execute kill -9 while receive ` | 1 hashing, 0 hashed (65.5 KB), 0 cached (0 B), uploaded 0 B, estimating...` message
	var wg sync.WaitGroup

	// Add a WaitGroup counter for the first goroutine
	wg.Add(1)

	go func() {
		defer wg.Done()

		// Create a scanner to read from stderrPipe
		scanner := bufio.NewScanner(stderrPipe)
		scanner.Split(bufio.ScanLines)

		for scanner.Scan() {
			output := scanner.Text()
			t.Log(output)

			// Check if the output contains the "hashing" etc.
			if strings.Contains(output, "hashing") && strings.Contains(output, "hashed") && strings.Contains(output, "uploaded") {
				t.Logf("Detaching and terminating target process")
				cmd.Process.Kill()

				break
			}
		}
	}()

	// Start the command
	err = cmd.Start()
	require.NoError(t, err)

	// Wait for the goroutines to finish
	wg.Wait()

	// Wait for the command
	cmd.Wait()
}

// CompareDirs examines and compares the quantities and contents of files in two different folders.
func CompareDirs(t *testing.T, source, destination string) {
	t.Helper()

	var buf bytes.Buffer

	ctx := context.Background()

	c, err := diff.NewComparer(&buf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	e1, err := localfs.NewEntry(source)
	require.NoError(t, err)

	e2, err := localfs.NewEntry(destination)
	require.NoError(t, err)

	err = c.Compare(ctx, e1, e2)
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
