//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package recovery

import (
	"bufio"
	"bytes"
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

// TestConsistencyWhenKill9AfterModify will test the data consistency while it encounterd kill -9 signal
func TestConsistencyWhenKill9AfterModify(t *testing.T) {
	// assumption: the test is run on filesystem & not directly on object store
	dataRepoPath := path.Join(*repoPathPrefix, dirPath, dataPath)

	baseDir := t.TempDir()
	if baseDir == "" {
		t.FailNow()
	}

	bm, err := blobmanipulator.NewBlobManipulator(baseDir, dataRepoPath)
	if err != nil {
		if errors.Is(err, kopiarunner.ErrExeVariableNotSet) {
			t.Skip("Skipping crash consistency tests because KOPIA_EXE is not set")
		} else {
			t.Skip("Error creating SnapshotTester:", err)
		}
	}

	bm.DataRepoPath = dataRepoPath

	// create a snapshot for initialized data
	snapID, err := bm.SetUpSystemWithOneSnapshot()
	if err != nil {
		t.FailNow()
	}
	cmpDir := bm.PathToTakeSnapshot

	copyDir := t.TempDir()
	if copyDir == "" {
		t.FailNow()
	}

	err = bm.FileHandler.CopyAllFiles(cmpDir, copyDir)
	require.NoError(t, err)

	// add files
	fileSize := 1 * 1024 * 1024
	numFiles := 200

	err = bm.GenerateRandomFiles(fileSize, numFiles)
	require.NoError(t, err)

	err = bm.FileHandler.CopyAllFiles(bm.PathToTakeSnapshot, copyDir)
	require.NoError(t, err)

	// modify original files
	content := "\nthis is a test for TestConsistencyWhenKill9AfterModify\n"
	err = bm.FileHandler.ModifyDataSetWithContent(copyDir, content)
	require.NoError(t, err)
	log.Println("Copy content and modify the files.")

	// kill the kopia command before it exits
	kopiaExe := os.Getenv("KOPIA_EXE")

	cmd := exec.Command(kopiaExe, "repo", "connect", "filesystem", "--path="+dataRepoPath, "--content-cache-size-mb", "500", "--metadata-cache-size-mb", "500", "--no-check-for-updates")
	env := []string{"KOPIA_PASSWORD=" + testenv.TestRepoPassword}
	cmd.Env = append(os.Environ(), env...)

	o, err := cmd.CombinedOutput()
	require.NoError(t, err)
	log.Println(string(o))

	cmd = exec.Command(kopiaExe, "snap", "create", copyDir, "--json", "--parallel=1")

	log.Println("Kill the kopia command before it exits:")
	killOnCondition(t, cmd)

	log.Println("Verify snapshot corruption:")
	// verify snapshot corruption
	err = bm.VerifySnapshot()
	require.NoError(t, err)

	// Create a temporary dir to restore a snapshot
	restoreDir := t.TempDir()
	if restoreDir == "" {
		t.FailNow()
	}

	// try to restore a snapshot without any error messages.
	stdout, err := bm.RestoreGivenOrRandomSnapshot(snapID, restoreDir)
	require.NoError(t, err)

	log.Println(stdout)

	log.Println("Compare restored data and original data:")
	err = bm.FileHandler.CompareDirs(restoreDir, cmpDir)
	require.NoError(t, err)
}

func killOnCondition(t *testing.T, cmd *exec.Cmd) {
	stderrPipe, err := cmd.StderrPipe()
	require.NoError(t, err)

	// excute kill -9 while recieve ` | 1 hashing, 0 hashed (65.5 KB), 0 cached (0 B), uploaded 0 B, estimating...` message
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Add a WaitGroup counter for the first goroutine
	wg.Add(1)

	errOut := bytes.Buffer{}

	go func() {
		mu.Lock()
		defer mu.Unlock()
		defer wg.Done()

		// Create a scanner to read from stderrPipe
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			output := scanner.Text()
			log.Println(output)
			errOut.Write(scanner.Bytes())
			errOut.WriteByte('\n')

			log.Println(output)
			// Check if the output contains the "hashing" etc.
			if strings.Contains(output, "hashing") && strings.Contains(output, "hashed") && strings.Contains(output, "uploaded") || strings.Contains(output, "Snapshotting") {
				log.Println("Detaching and terminating target process")
				cmd.Process.Kill()
				break
			}
		}
	}()

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		panic(err)
	}

	wg.Add(1)

	o := bytes.Buffer{}

	go func() {
		defer wg.Done()

		// Create a scanner to read from stdoutPipe
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			output := scanner.Text()
			log.Println(output)
			o.Write(scanner.Bytes())
			o.WriteByte('\n')

			log.Println("snapshot create successfully", output)
			// Check if the output contains the "copying" text
			if strings.Contains(output, "hashing") && strings.Contains(output, "hashed") && strings.Contains(output, "uploaded") {
				cmd.Process.Kill()
				break
			}
		}
	}()

	// Run the command
	err = cmd.Run()

	// Wait for the goroutines to finish
	wg.Wait()
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
